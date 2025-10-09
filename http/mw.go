package http

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RequestIDHeader is the name of the HTTP Header which contains the request id.
const (
	RequestIDHeader = "X-Request-Id"
	ApiKeyHeader    = "X-API-Key"
)

type (
	Middleware      func(http.Handler) http.Handler
	MiddlewareFunc  func(http.HandlerFunc) http.HandlerFunc
	requestIdCtxKey struct{}
)

type ResponseWriter struct {
	writer  http.ResponseWriter
	flusher http.Flusher
	status  int
	written int
	user    string
}

var (
	_ http.ResponseWriter = (*ResponseWriter)(nil)
	_ http.Flusher        = (*ResponseWriter)(nil)
)

func (w *ResponseWriter) Header() http.Header {
	return w.writer.Header()
}

func (w *ResponseWriter) WriteHeader(statusCode int) {
	w.writer.WriteHeader(statusCode)
	w.status = statusCode
}

func (w *ResponseWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.written += n
	return n, err
}

func (w *ResponseWriter) Flush() {
	w.flusher.Flush()
}

func (w *ResponseWriter) GroupedStatus() int {
	return w.status / 100 * 100
}

func WrapResponseWriter(rw http.ResponseWriter) (*ResponseWriter, error) {
	wrapped, ok := rw.(*ResponseWriter)
	if ok {
		return wrapped, nil
	}

	flusher, ok := rw.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("ResponseWriter does not implement http.Flusher")
	}

	return &ResponseWriter{
		writer:  rw,
		flusher: flusher,
		status:  http.StatusOK,
		written: 0,
	}, nil
}

func MiddlewareChain(mws ...Middleware) Middleware {
	return func(next http.Handler) http.Handler {
		for i := len(mws) - 1; i >= 0; i-- {
			next = mws[i](next)
		}
		return next
	}
}

func MiddlewareChainFunc(mws ...Middleware) MiddlewareFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		for i := len(mws) - 1; i >= 0; i-- {
			next = mws[i](next).ServeHTTP
		}
		return next
	}
}

func MiddlewareRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("Recovered panic", "recover", rec, "stack", string(debug.Stack()))
			}
		}()

		next.ServeHTTP(rw, r)
	})
}

func MiddlewareRequestID(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		requestID := r.Header.Get(RequestIDHeader)
		if requestID != "" {
			ctx = context.WithValue(ctx, requestIdCtxKey{}, requestID)
		} else if id, err := uuid.NewRandom(); err == nil {
			ctx = context.WithValue(ctx, requestIdCtxKey{}, id.String())
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	}
	return http.HandlerFunc(fn)
}

func MiddlewareLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		start := time.Now()

		wrapped, err := WrapResponseWriter(rw)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		next.ServeHTTP(wrapped, req)

		logger := slog.Default()

		ctx := req.Context()
		if reqID, ok := ctx.Value(requestIdCtxKey{}).(string); ok {
			logger = logger.With("request_id", reqID)
		}

		var logLevel slog.Level
		switch {
		case 400 <= wrapped.status && wrapped.status < 500:
			logLevel = slog.LevelWarn
		case 500 <= wrapped.status:
			logLevel = slog.LevelError
		default:
			logLevel = slog.LevelInfo

		}

		logger.Log(ctx, logLevel, "Served",
			"method", req.Method,
			"path", req.URL.Path,
			"time", time.Since(start),
			"status", wrapped.status,
			"size", wrapped.written,
			"user", wrapped.user,
		)
	})
}

func MiddlewareMetric(provider metric.MeterProvider) Middleware {
	meter := provider.Meter("atlas")

	requestCount, err := meter.Int64Counter("requests")
	if err != nil {
		panic(fmt.Errorf("init requests int64 counter: %w", err))
	}

	inflight, err := meter.Int64Gauge("in_flight")
	if err != nil {
		panic(fmt.Errorf("init in_flight int64 counter: %w", err))
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			ctx := r.Context()

			inflight.Record(ctx, 1)
			defer inflight.Record(ctx, -1)

			wrapped, err := WrapResponseWriter(rw)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}

			next.ServeHTTP(wrapped, r)

			requestCount.Add(ctx, 1, metric.WithAttributes(attribute.Int("status", wrapped.GroupedStatus())))
		})
	}
}

func MiddlewareAuthentication(keys []string, users []string) func(next http.Handler) http.Handler {
	lookup := make(map[string]string, len(keys))
	for i, key := range keys {
		lookup[key] = users[i]
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			key := req.Header.Get(ApiKeyHeader)
			if key == "" {
				EncodeErr(rw, http.StatusUnauthorized, fmt.Sprintf("please set the %s header", ApiKeyHeader))
				return
			}

			user, found := lookup[key]
			if !found {
				EncodeErr(rw, http.StatusUnauthorized, fmt.Sprintf("unrecognized API key %q", key))
				return
			}

			wrapped, err := WrapResponseWriter(rw)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusInternalServerError)
				return
			}

			wrapped.user = user

			next.ServeHTTP(wrapped, req)
		})
	}
}

func MiddlewareContentType(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		ext := filepath.Ext(r.URL.Path)
		if ext == ".yaml" || ext == ".yml" {
			rw.Header().Set("Content-Type", "application/x-yaml")
		}

		next.ServeHTTP(rw, r)
	})
}

// MiddlewareGZip applies gzip compression to HTTP responses if the client supports it.
func MiddlewareGZip(next http.Handler) http.Handler {
	gzPool := sync.Pool{
		New: func() interface{} {
			// For web content, level 4 seems to be a sweet spot.
			// https://github.com/gin-contrib/gzip/blob/3b246bb1ab0a98b1e3685d98711669008e26d84a/handler.go#L42
			gz, err := gzip.NewWriterLevel(io.Discard, 4)
			if err != nil {
				panic(fmt.Errorf("failed to create gzip writer: %w", err))
			}
			return gz
		},
	}

	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		// Check if the client accepts gzip encoding.
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(rw, r)
			return
		}

		// Prevent applying compression to "upgrade" requests (e.g., WebSocket).
		if strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
			next.ServeHTTP(rw, r)
			return
		}

		// Wrap the ResponseWriter.
		gzw := gzPool.Get().(*gzip.Writer)
		defer gzw.Close()
		defer gzPool.Put(gzw)

		// Configure the gzip writer to write to the http ResponseWriter
		gzw.Reset(rw)

		// Create an http.ResponseWriter that writes to the gzip writer
		gzrw := &gzipResponseWriter{
			ResponseWriter: rw,
			gzw:            gzw,
		}

		// Wrap the gzip response writer again to have access to the "written" field
		wrapped, err := WrapResponseWriter(gzrw)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		// Add gzip headers.
		wrapped.Header().Set("Content-Encoding", "gzip")
		wrapped.Header().Add("Vary", "Accept-Encoding")

		// check ETag Header
		etag := r.Header.Get("ETag")
		if etag != "" && !strings.HasPrefix(etag, "W/") {
			wrapped.Header().Set("ETag", "W/"+etag)
		}

		defer func() {
			if wrapped.written < 0 {
				gzw.Reset(io.Discard) // do not write gzip footer when nothing is written to the response body
			}
			if wrapped.written > -1 {
				wrapped.Header().Set("Content-Length", strconv.Itoa(wrapped.written))
			}
		}()

		next.ServeHTTP(wrapped, r)
	})
}

// gzipResponseWriter wraps http.ResponseWriter to provide gzip compression.
type gzipResponseWriter struct {
	http.ResponseWriter
	gzw *gzip.Writer
}

func (g *gzipResponseWriter) Write(data []byte) (int, error) {
	return g.gzw.Write(data)
}

func (g *gzipResponseWriter) Flush() {
	flusher, ok := g.ResponseWriter.(http.Flusher)
	if ok {
		flusher.Flush()
	}
}
