package http

import (
	"bytes"
	"context"
	"errors"
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

func TestClientDownloadLogsProgress(t *testing.T) {
	srv, _ := newFileServer(t, []byte(strings.Repeat("x", 1000)), `"v1"`)
	c := newClient(t, DefaultClientConfig())

	var buf bytes.Buffer
	cfg := DefaultDownloadConfig()
	cfg.Logger = slog.New(slog.NewTextHandler(&buf, nil))
	cfg.ProgressInterval = time.Nanosecond

	if err := c.Download(context.Background(), srv.URL, filepath.Join(t.TempDir(), "file"), cfg); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"msg=Downloading", "total=1000", "percent=100"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("log lacks %q:\n%s", want, buf.String())
		}
	}
}

func TestDownloadConfigValidate(t *testing.T) {
	zero := DefaultDownloadConfig()
	zero.ProgressInterval = 0

	tests := []struct {
		name    string
		cfg     *DownloadConfig
		wantErr bool
	}{
		{name: "default", cfg: DefaultDownloadConfig(), wantErr: false},
		{name: "nil", cfg: nil, wantErr: true},
		{name: "zero progress interval", cfg: zero, wantErr: true},
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
