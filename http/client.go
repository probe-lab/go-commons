package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/time/rate"
)

// DefaultUserAgent identifies the client when the configuration sets none.
const DefaultUserAgent = "go-commons (+https://github.com/probe-lab/go-commons)"

// meterName is the instrumentation scope of the metrics in this package.
const meterName = "github.com/probe-lab/go-commons/http"

type ClientConfig struct {
	// UserAgent is sent with every request.
	UserAgent string
	// Timeout bounds one request from dial to the end of the body.
	Timeout time.Duration
	// MaxBody caps the number of response bytes read; larger bodies are an
	// error.
	MaxBody int64
	// MaxRedirects caps the redirects followed for one request.
	MaxRedirects int
	// RPS caps requests per second across all hosts; 0 means no limit.
	RPS float64
}

func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		UserAgent:    DefaultUserAgent,
		Timeout:      15 * time.Second,
		MaxBody:      4 << 20,
		MaxRedirects: 5,
		RPS:          0,
	}
}

func (cfg *ClientConfig) Validate() error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	if cfg.UserAgent == "" {
		return fmt.Errorf("user agent must not be empty")
	}

	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}

	if cfg.MaxBody <= 0 {
		return fmt.Errorf("max body must be positive")
	}

	if cfg.MaxRedirects < 0 {
		return fmt.Errorf("max redirects must not be negative")
	}

	if cfg.RPS < 0 {
		return fmt.Errorf("rps must not be negative")
	}

	return nil
}

// Client wraps http.Client with one user agent, one timeout, a redirect cap,
// a response size cap, and an optional global rate limit. Requests are
// traced and measured by the OpenTelemetry HTTP instrumentation.
type Client struct {
	hc        *http.Client
	userAgent string
	maxBody   int64
	limiter   *rate.Limiter
}

// NewClient builds a Client from a validated configuration.
func NewClient(cfg *ClientConfig) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: cfg.Timeout,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       30 * time.Second,
		ForceAttemptHTTP2:     true,
	}

	maxRedirects := cfg.MaxRedirects
	var limiter *rate.Limiter
	if cfg.RPS > 0 {
		limiter = rate.NewLimiter(rate.Limit(cfg.RPS), int(cfg.RPS)+1)
	}

	return &Client{
		hc: &http.Client{
			Transport: otelhttp.NewTransport(transport),
			Timeout:   cfg.Timeout,
			CheckRedirect: func(_ *http.Request, via []*http.Request) error {
				if len(via) >= maxRedirects {
					return fmt.Errorf("stopped after %d redirects", maxRedirects)
				}
				return nil
			},
		},
		userAgent: cfg.UserAgent,
		maxBody:   cfg.MaxBody,
		limiter:   limiter,
	}, nil
}

// Transport exposes the underlying round tripper for callers that need an
// uncapped download with the same connection settings.
func (c *Client) Transport() http.RoundTripper { return c.hc.Transport }

// Wait blocks until the rate limiter admits one request.
func (c *Client) Wait(ctx context.Context) error {
	if c.limiter == nil {
		return nil
	}
	return c.limiter.Wait(ctx)
}

// ClientRequest describes one call for Do.
type ClientRequest struct {
	// Method is the HTTP method; GET when empty.
	Method string
	// URL is the request URL.
	URL string
	// Body is the request body; nil sends none.
	Body []byte
	// ContentType is sent as the Content-Type header when not empty.
	ContentType string
	// Accept is sent as the Accept header when not empty.
	Accept string
	// Header holds extra headers added to the request.
	Header http.Header
}

// ClientResponse is the part of an HTTP response most callers care about.
type ClientResponse struct {
	// URL is the final URL after redirects.
	URL string
	// StatusCode is the HTTP status code.
	StatusCode int
	// ContentType is the media type of the body without parameters.
	ContentType string
	// Header holds the response headers.
	Header http.Header
	// Body is the response body, capped at the configured size.
	Body []byte
	// TLS reports whether the response came over TLS.
	TLS bool
}

// StatusError is returned by Do for non-2xx responses.
type StatusError struct {
	URL         string
	StatusCode  int
	ContentType string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s: status %d", e.URL, e.StatusCode)
}

// NotFound reports whether the status is 404 or 410.
func (e *StatusError) NotFound() bool {
	return e.StatusCode == http.StatusNotFound || e.StatusCode == http.StatusGone
}

// IsNotFound reports whether err is a StatusError for a 404 or 410.
func IsNotFound(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.NotFound()
}

// AsStatus returns the status code and content type if err is a StatusError.
func AsStatus(err error) (int, string, bool) {
	var se *StatusError
	if errors.As(err, &se) {
		return se.StatusCode, se.ContentType, true
	}
	return 0, "", false
}

// Get fetches url with the given Accept header and returns the body, capped
// at the configured size. Non-2xx responses return a *StatusError but the
// ClientResponse is still returned so callers can inspect headers.
func (c *Client) Get(ctx context.Context, url, accept string) (*ClientResponse, error) {
	return c.Do(ctx, ClientRequest{Method: http.MethodGet, URL: url, Accept: accept})
}

// GetJSON fetches url and decodes the body into v. It accepts any body that
// parses as JSON, whatever the Content-Type says.
func (c *Client) GetJSON(ctx context.Context, url string, v any) (*ClientResponse, error) {
	res, err := c.Get(ctx, url, "application/json, */*;q=0.5")
	if err != nil {
		return res, err
	}
	if err := json.Unmarshal(res.Body, v); err != nil {
		return res, fmt.Errorf("decode %s: %w", url, err)
	}
	return res, nil
}

// Do sends the request and returns the body, capped at the configured size.
// Non-2xx responses return a *StatusError together with the ClientResponse,
// so callers can read the status and headers.
func (c *Client) Do(ctx context.Context, r ClientRequest) (*ClientResponse, error) {
	method := r.Method
	if method == "" {
		method = http.MethodGet
	}
	if err := c.Wait(ctx); err != nil {
		return nil, err
	}
	var body io.Reader
	if r.Body != nil {
		body = bytes.NewReader(r.Body)
	}
	req, err := http.NewRequestWithContext(ctx, method, r.URL, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	if r.Accept != "" {
		req.Header.Set("Accept", r.Accept)
	}
	if r.ContentType != "" {
		req.Header.Set("Content-Type", r.ContentType)
	}
	for k, vs := range r.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	res, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	data, err := io.ReadAll(io.LimitReader(res.Body, c.maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("read body of %s: %w", r.URL, err)
	}
	if int64(len(data)) > c.maxBody {
		return nil, fmt.Errorf("body of %s exceeds %d bytes", r.URL, c.maxBody)
	}
	out := &ClientResponse{
		URL:         res.Request.URL.String(),
		StatusCode:  res.StatusCode,
		ContentType: mediaType(res.Header.Get("Content-Type")),
		Header:      res.Header,
		Body:        data,
		TLS:         res.TLS != nil,
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return out, &StatusError{URL: r.URL, StatusCode: res.StatusCode, ContentType: out.ContentType}
	}
	return out, nil
}

func mediaType(ct string) string {
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return strings.ToLower(strings.TrimSpace(strings.Split(ct, ";")[0]))
	}
	return mt
}
