package http

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// requestLog records the headers of the requests a test server receives.
type requestLog struct {
	mu      sync.Mutex
	headers []http.Header
}

func (l *requestLog) add(h http.Header) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.headers = append(l.headers, h)
}

func (l *requestLog) all() []http.Header {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.headers)
}

// newFileServer serves body with an ETag and answers Range and If-Range
// requests like a file server.
func newFileServer(t *testing.T, body []byte, etag string) (*httptest.Server, *requestLog) {
	t.Helper()

	rl := &requestLog{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rl.add(r.Header.Clone())
		w.Header().Set("ETag", etag)
		http.ServeContent(w, r, "file", time.Time{}, bytes.NewReader(body))
	}))
	t.Cleanup(srv.Close)

	return srv, rl
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestClientDownload(t *testing.T) {
	body := []byte(strings.Repeat("0123456789", 100))
	const etag = `"v2"`

	tests := []struct {
		name      string
		final     string // content at path before the download; empty means none
		part      string // content of the part file; empty means none
		partETag  string // ETag stored with the part file
		wantRange string // Range header of the request; empty means none
		requests  int
		want      string
	}{
		{name: "new download", requests: 1, want: string(body)},
		{name: "complete file", final: "kept", requests: 0, want: "kept"},
		{name: "resume", part: string(body[:400]), partETag: etag, wantRange: "bytes=400-", requests: 1, want: string(body)},
		{name: "file changed on the server", part: "stale data", partETag: `"v1"`, wantRange: "bytes=10-", requests: 1, want: string(body)},
		{name: "part holds the whole body", part: string(body), partETag: etag, wantRange: "bytes=1000-", requests: 1, want: string(body)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, rl := newFileServer(t, body, etag)
			c := newClient(t, DefaultClientConfig())

			path := filepath.Join(t.TempDir(), "dir", "file.csv")
			if tt.final != "" {
				writeFile(t, path, tt.final)
			}
			if tt.part != "" {
				writeFile(t, path+".part", tt.part)
				writeFile(t, path+".part.etag", tt.partETag)
			}

			if err := c.Download(context.Background(), srv.URL, path, DefaultDownloadConfig()); err != nil {
				t.Fatalf("Download: %v", err)
			}

			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tt.want {
				t.Errorf("file holds %d bytes, want %d", len(got), len(tt.want))
			}

			for _, leftover := range []string{path + ".part", path + ".part.etag"} {
				if _, err := os.Stat(leftover); !errors.Is(err, fs.ErrNotExist) {
					t.Errorf("%s is left over (stat error %v)", filepath.Base(leftover), err)
				}
			}

			headers := rl.all()
			if len(headers) != tt.requests {
				t.Fatalf("requests = %d, want %d", len(headers), tt.requests)
			}
			if tt.requests == 0 {
				return
			}

			h := headers[0]
			if got := h.Get("Range"); got != tt.wantRange {
				t.Errorf("Range = %q, want %q", got, tt.wantRange)
			}
			if got := h.Get("User-Agent"); got != DefaultUserAgent {
				t.Errorf("User-Agent = %q, want %q", got, DefaultUserAgent)
			}
			if got := h.Get("Accept-Encoding"); got != "identity" {
				t.Errorf("Accept-Encoding = %q, want identity", got)
			}
		})
	}
}

func TestClientDownloadStatusError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	c := newClient(t, DefaultClientConfig())
	path := filepath.Join(t.TempDir(), "file")

	err := c.Download(context.Background(), srv.URL, path, DefaultDownloadConfig())
	if !IsNotFound(err) {
		t.Fatalf("Download error = %v, want a 404 StatusError", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("file exists after a failed download (stat error %v)", err)
	}
}

// progressLog records the progress reports of a download.
type progressLog struct {
	reports []DownloadProgress
}

func (l *progressLog) add(p DownloadProgress) {
	l.reports = append(l.reports, p)
}

func TestClientDownloadReportsProgress(t *testing.T) {
	body := []byte(strings.Repeat("x", 1000))
	const etag = `"v1"`

	tests := []struct {
		name   string
		offset int // bytes in the part file before the download
	}{
		{name: "new download", offset: 0},
		{name: "resume", offset: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newFileServer(t, body, etag)
			c := newClient(t, DefaultClientConfig())

			path := filepath.Join(t.TempDir(), "file")
			if tt.offset > 0 {
				writeFile(t, path+".part", string(body[:tt.offset]))
				writeFile(t, path+".part.etag", etag)
			}

			pl := &progressLog{}
			cfg := DefaultDownloadConfig()
			cfg.OnProgress = pl.add
			cfg.ProgressInterval = 0

			if err := c.Download(context.Background(), srv.URL, path, cfg); err != nil {
				t.Fatal(err)
			}

			if len(pl.reports) < 2 {
				t.Fatalf("got %d reports, want at least 2: %+v", len(pl.reports), pl.reports)
			}

			first := pl.reports[0]
			first.Elapsed = 0
			wantFirst := DownloadProgress{URL: srv.URL, Offset: int64(tt.offset), Done: int64(tt.offset), Total: 1000}
			if first != wantFirst {
				t.Errorf("first report = %+v, want %+v", first, wantFirst)
			}

			if last := pl.reports[len(pl.reports)-1]; last.Done != 1000 || last.Total != 1000 {
				t.Errorf("last report = %+v, want Done and Total 1000", last)
			}

			for i := 1; i < len(pl.reports); i++ {
				if pl.reports[i].Done <= pl.reports[i-1].Done {
					t.Errorf("report %d has Done %d after %d", i, pl.reports[i].Done, pl.reports[i-1].Done)
				}
			}
		})
	}
}

// slowReader returns one byte per read, one second after the read starts.
type slowReader struct {
	left int
}

func (r *slowReader) Read(b []byte) (int, error) {
	if r.left == 0 {
		return 0, io.EOF
	}

	time.Sleep(time.Second)
	r.left--
	b[0] = 'x'

	return 1, nil
}

func TestProgressReaderInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		want     []int64 // Done of each report
	}{
		{name: "each read", interval: 0, want: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
		{name: "every 3s", interval: 3 * time.Second, want: []int64{0, 3, 6, 9, 10}},
		{name: "last interval ends with the body", interval: 10 * time.Second, want: []int64{0, 10}},
		{name: "interval longer than the body", interval: time.Minute, want: []int64{0, 10}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pl := &progressLog{}
				pr := newProgressReader(&slowReader{left: 10}, pl.add, tt.interval, DownloadProgress{Total: 10})

				pr.report()
				if _, err := io.Copy(io.Discard, pr); err != nil {
					t.Fatal(err)
				}
				pr.finish()

				var got []int64
				for _, p := range pl.reports {
					got = append(got, p.Done)
				}
				if !slices.Equal(got, tt.want) {
					t.Errorf("reports at Done %v, want %v", got, tt.want)
				}

				if last := pl.reports[len(pl.reports)-1]; last.Elapsed != 10*time.Second {
					t.Errorf("last report Elapsed = %v, want 10s", last.Elapsed)
				}
			})
		})
	}
}

func TestProgressReaderNilFunc(t *testing.T) {
	pr := newProgressReader(strings.NewReader("data"), nil, 0, DownloadProgress{})

	pr.report()
	n, err := io.Copy(io.Discard, pr)
	if err != nil || n != 4 {
		t.Fatalf("Copy = %d, %v; want 4, nil", n, err)
	}
	pr.finish()
}

func dropTime(_ []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey {
		return slog.Attr{}
	}
	return a
}

func TestLogProgress(t *testing.T) {
	tests := []struct {
		name string
		p    DownloadProgress
		want string
	}{
		{
			name: "start of a new download",
			p:    DownloadProgress{URL: "u", Done: 0, Total: 1000},
			want: "level=INFO msg=Downloading url=u bytes=0 total=1000 percent=0",
		},
		{
			name: "start of a resumed download",
			p:    DownloadProgress{URL: "u", Offset: 400, Done: 400, Total: 1000},
			want: `level=INFO msg="Resuming download" url=u bytes=400 total=1000 percent=40`,
		},
		{
			name: "resumed download in progress",
			p:    DownloadProgress{URL: "u", Offset: 400, Done: 500, Total: 1000},
			want: "level=INFO msg=Downloading url=u bytes=500 total=1000 percent=50",
		},
		{
			name: "unknown total",
			p:    DownloadProgress{URL: "u", Done: 500, Total: -1},
			want: "level=INFO msg=Downloading url=u bytes=500",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: dropTime}))

			LogProgress(logger)(tt.p)

			if got := strings.TrimSpace(buf.String()); got != tt.want {
				t.Errorf("log = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDownloadConfigValidate(t *testing.T) {
	zero := DefaultDownloadConfig()
	zero.ProgressInterval = 0

	negative := DefaultDownloadConfig()
	negative.ProgressInterval = -time.Second

	tests := []struct {
		name    string
		cfg     *DownloadConfig
		wantErr bool
	}{
		{name: "default", cfg: DefaultDownloadConfig(), wantErr: false},
		{name: "nil", cfg: nil, wantErr: true},
		{name: "zero progress interval", cfg: zero, wantErr: false},
		{name: "negative progress interval", cfg: negative, wantErr: true},
		{name: "no progress reports", cfg: &DownloadConfig{}, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseContentRange(t *testing.T) {
	tests := []struct {
		header    string
		wantStart int64
		wantTotal int64
		wantOK    bool
	}{
		{header: "bytes 100-999/1000", wantStart: 100, wantTotal: 1000, wantOK: true},
		{header: "bytes */1000", wantStart: -1, wantTotal: 1000, wantOK: true},
		{header: "bytes 0-9/*", wantStart: 0, wantTotal: -1, wantOK: true},
		{header: "items 0-9/10", wantOK: false},
		{header: "bytes 0-9", wantOK: false},
		{header: "bytes x-9/10", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.header, func(t *testing.T) {
			start, total, ok := parseContentRange(tt.header)
			if ok != tt.wantOK || (ok && (start != tt.wantStart || total != tt.wantTotal)) {
				t.Errorf("parseContentRange(%q) = %d, %d, %v; want %d, %d, %v", tt.header, start, total, ok, tt.wantStart, tt.wantTotal, tt.wantOK)
			}
		})
	}
}
