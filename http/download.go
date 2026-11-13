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
	// Logger receives the progress messages; nil means slog.Default().
	Logger *slog.Logger
	// ProgressInterval is the time between two progress messages.
	ProgressInterval time.Duration
}

// DefaultDownloadConfig returns a configuration that logs progress every 10
// seconds.
func DefaultDownloadConfig() *DownloadConfig {
	return &DownloadConfig{
		ProgressInterval: 10 * time.Second,
	}
}

// Validate reports the first invalid value of the configuration.
func (cfg *DownloadConfig) Validate() error {
	if cfg == nil {
		return errors.New("config is nil")
	}

	if cfg.ProgressInterval <= 0 {
		return errors.New("progress interval must be positive")
	}

	return nil
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
// MaxBody; cancel ctx to stop it. It logs the progress every
// ProgressInterval. A non-2xx response returns a *StatusError.
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

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
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
	defer log.Defer(res.Body.Close, "Failed to close response body")

	switch res.StatusCode {
	case http.StatusPartialContent:
		start, _, ok := parseContentRange(res.Header.Get("Content-Range"))
		if !ok || start != offset {
			return fmt.Errorf("GET %s: server resumed at %q, wanted byte %d", url, res.Header.Get("Content-Range"), offset)
		}
		logger.Info("Resuming download", "url", redactURL(url), "offset", offset)

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

	pr := &progressReader{
		r:        res.Body,
		url:      url,
		done:     offset,
		total:    total,
		log:      logger,
		interval: cfg.ProgressInterval,
		last:     time.Now(),
	}
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

// progressReader logs how far a download has come, at most once per interval.
type progressReader struct {
	r        io.Reader
	url      string
	done     int64
	total    int64
	log      *slog.Logger
	interval time.Duration
	last     time.Time
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.done += int64(n)

	if now := time.Now(); now.Sub(p.last) >= p.interval {
		p.last = now

		attrs := []any{"url", redactURL(p.url), "bytes", p.done}
		if p.total > 0 {
			attrs = append(attrs, "total", p.total, "percent", p.done*100/p.total)
		}

		p.log.Info("Downloading", attrs...)
	}

	return n, err
}
