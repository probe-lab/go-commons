package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/probe-lab/go-commons/log"
)

// DownloadConfig configures Client.Download.
type DownloadConfig struct {
	// Header holds extra request headers, such as Authorization. A header
	// given here replaces the one Download sets.
	Header http.Header

	// OnProgress receives the progress of the body: once when the body
	// starts, at most once per ProgressInterval while it arrives, and once
	// when it is complete. It runs on the goroutine of Download, so a slow
	// function slows the download. Nil means no reports.
	OnProgress func(DownloadProgress)

	// ProgressInterval is the minimum time between two calls of OnProgress.
	// Zero calls OnProgress after each read from the body.
	ProgressInterval time.Duration
}

// DefaultDownloadConfig logs the progress with slog.Default() every 10
// seconds.
func DefaultDownloadConfig() *DownloadConfig {
	return &DownloadConfig{
		OnProgress:       LogProgress(nil),
		ProgressInterval: 10 * time.Second,
	}
}

func (cfg *DownloadConfig) Validate() error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	if cfg.ProgressInterval < 0 {
		return fmt.Errorf("progress interval must not be negative")
	}

	return nil
}

// DownloadProgress is one progress report of Client.Download.
type DownloadProgress struct {
	// URL is the URL that Download fetches.
	URL string

	// Offset is the size of the part file when the body started. It is
	// larger than zero when Download resumes an earlier download.
	Offset int64

	// Done is the number of bytes on disk, Offset included.
	Done int64

	// Total is the size of the complete file, or -1 when the server sent no
	// length.
	Total int64

	// Elapsed is the time since the body started.
	Elapsed time.Duration
}

// LogProgress returns an OnProgress function that logs each report at the
// info level. A nil logger means slog.Default() at the time of each report.
func LogProgress(logger *slog.Logger) func(DownloadProgress) {
	return func(p DownloadProgress) {
		l := logger
		if l == nil {
			l = slog.Default()
		}

		attrs := []any{"url", p.URL, "bytes", p.Done}
		if p.Total > 0 {
			attrs = append(attrs, "total", p.Total, "percent", p.Done*100/p.Total)
		}

		var msg string
		switch {
		case p.Offset > 0 && p.Done == p.Offset: // the first report of a resumed download
			msg = "Resuming download"
		default:
			msg = "Downloading"
		}

		l.Info(msg, attrs...)
	}
}

// Download writes the body of a GET request for url to path and resumes an
// earlier download of the same path that stopped.
//
// The body goes to path + ".part" and the ETag of the response to
// path + ".part.etag". When a part file exists, Download asks for the rest
// with a Range request; If-Range with the stored ETag makes the server send
// the whole body again when the file changed in the meantime. Only a
// complete body is synced to disk and renamed to path, so a file at path is
// complete and Download returns at once, without a request. A failed
// download keeps the part file for the next call.
//
// Download waits for the rate limit, sends the user agent of the client and
// follows redirects up to the cap. It asks for the identity encoding, so the
// byte offsets of a resumed download stay valid. It ignores Timeout and
// MaxBody; cancel ctx to stop it. It reports the progress of the body to
// OnProgress; a complete file at path gives no reports. A non-2xx response
// returns a *StatusError.
func (c *Client) Download(ctx context.Context, url, path string, cfg *DownloadConfig) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validate config: %w", err)
	}

	switch _, err := os.Stat(path); {
	case err == nil:
		return nil
	case !errors.Is(err, fs.ErrNotExist):
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	part := path + ".part"
	etagPath := part + ".etag"

	var offset int64
	if fi, err := os.Stat(part); err == nil {
		offset = fi.Size()
	}

	res, err := c.downloadRequest(ctx, url, offset, etagPath, cfg.Header)
	if err != nil {
		return err
	}
	defer log.Defer(res.Body.Close, "failed to close response body")

	switch res.StatusCode {
	case http.StatusPartialContent:
		start, _, ok := parseContentRange(res.Header.Get("Content-Range"))
		if !ok || start != offset {
			return fmt.Errorf("GET %s: server resumed at %q, wanted byte %d", url, res.Header.Get("Content-Range"), offset)
		}

	case http.StatusOK:
		offset = 0

	case http.StatusRequestedRangeNotSatisfiable:
		// The part file may already hold the whole body.
		if _, total, ok := parseContentRange(res.Header.Get("Content-Range")); ok && total == offset {
			return finishDownload(part, etagPath, path)
		}
		if err := errors.Join(os.Remove(part), os.Remove(etagPath)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return c.Download(ctx, url, path, cfg)

	default:
		return &StatusError{URL: url, StatusCode: res.StatusCode, ContentType: mediaType(res.Header.Get("Content-Type"))}
	}

	flags := os.O_CREATE | os.O_WRONLY
	if offset > 0 {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
		if err := writeETag(etagPath, res.Header.Get("ETag")); err != nil {
			return err
		}
	}

	fh, err := os.OpenFile(part, flags, 0o644)
	if err != nil {
		return err
	}

	total := int64(-1)
	if res.ContentLength >= 0 {
		total = offset + res.ContentLength
	}

	pr := newProgressReader(res.Body, cfg.OnProgress, cfg.ProgressInterval, DownloadProgress{
		URL:    url,
		Offset: offset,
		Done:   offset,
		Total:  total,
	})
	pr.report()

	n, err := io.Copy(fh, pr)
	if cerr := fh.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("download %s: %w (kept %s to resume)", url, err, part)
	}

	if res.ContentLength >= 0 && n != res.ContentLength {
		return fmt.Errorf("download %s: got %d of %d bytes (kept %s to resume)", url, n, res.ContentLength, part)
	}

	pr.finish()

	return finishDownload(part, etagPath, path)
}

func (c *Client) downloadRequest(ctx context.Context, url string, offset int64, etagPath string, header http.Header) (*http.Response, error) {
	if err := c.Wait(ctx); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept-Encoding", "identity")
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
		if etag, err := os.ReadFile(etagPath); err == nil && strings.TrimSpace(string(etag)) != "" {
			req.Header.Set("If-Range", strings.TrimSpace(string(etag)))
		}
	}
	for k, vs := range header {
		req.Header[http.CanonicalHeaderKey(k)] = vs
	}

	// The client of c has a timeout for one request; a download needs none.
	hc := &http.Client{Transport: c.hc.Transport, CheckRedirect: c.hc.CheckRedirect}
	return hc.Do(req)
}

// finishDownload syncs the part file to disk and moves it to path.
func finishDownload(part, etagPath, path string) error {
	fh, err := os.Open(part)
	if err != nil {
		return err
	}
	err = fh.Sync()
	if cerr := fh.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("sync %s: %w", part, err)
	}

	if err := os.Rename(part, path); err != nil {
		return err
	}

	if err := os.Remove(etagPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	return nil
}

func writeETag(path, etag string) error {
	if etag == "" {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return nil
	}

	return os.WriteFile(path, []byte(etag), 0o644)
}

// parseContentRange reads "bytes 100-999/1000" or "bytes */1000" and returns
// the first byte and the total size; start is -1 for "*" and total is -1 when
// the size is "*".
func parseContentRange(h string) (start, total int64, ok bool) {
	spec, ok := strings.CutPrefix(h, "bytes ")
	if !ok {
		return 0, 0, false
	}

	rng, size, ok := strings.Cut(spec, "/")
	if !ok {
		return 0, 0, false
	}

	total = -1
	if n, err := strconv.ParseInt(size, 10, 64); err == nil {
		total = n
	}

	if rng == "*" {
		return -1, total, true
	}

	first, _, ok := strings.Cut(rng, "-")
	if !ok {
		return 0, 0, false
	}

	start, err := strconv.ParseInt(first, 10, 64)
	if err != nil {
		return 0, 0, false
	}

	return start, total, true
}

// progressReader counts the bytes it reads from r and reports the progress to
// fn, at most once per interval.
type progressReader struct {
	r        io.Reader
	fn       func(DownloadProgress)
	interval time.Duration
	progress DownloadProgress
	started  time.Time
	lastAt   time.Time
	lastDone int64
}

func newProgressReader(r io.Reader, fn func(DownloadProgress), interval time.Duration, start DownloadProgress) *progressReader {
	return &progressReader{
		r:        r,
		fn:       fn,
		interval: interval,
		progress: start,
		started:  time.Now(),
	}
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.progress.Done += int64(n)

	if n > 0 && time.Since(p.lastAt) >= p.interval {
		p.report()
	}

	return n, err
}

// report calls fn with the current progress.
func (p *progressReader) report() {
	if p.fn == nil {
		return
	}

	now := time.Now()
	p.lastAt = now
	p.lastDone = p.progress.Done
	p.progress.Elapsed = now.Sub(p.started)
	p.fn(p.progress)
}

// finish reports the end of the body, unless the last report showed it.
func (p *progressReader) finish() {
	if p.progress.Done == p.lastDone {
		return
	}

	p.report()
}
