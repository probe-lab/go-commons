package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var (
	_ http.ResponseWriter = (*ResponseWriter)(nil)
	_ http.Flusher        = (*ResponseWriter)(nil)
)

func TestMiddlewareRecoverRespondsWithError(t *testing.T) {
	h := MiddlewareRecover(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if strings.Contains(rec.Body.String(), "boom") {
		t.Fatalf("body leaks the panic value: %q", rec.Body.String())
	}
}

func TestMiddlewareRecoverKeepsAbortHandler(t *testing.T) {
	h := MiddlewareRecover(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))

	defer func() {
		if rec := recover(); rec != http.ErrAbortHandler {
			t.Fatalf("recovered %v, want http.ErrAbortHandler", rec)
		}
	}()

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	t.Fatal("expected a panic")
}

func TestMiddlewareAuthentication(t *testing.T) {
	tests := []struct {
		name   string
		key    string
		status int
	}{
		{name: "missing key", key: "", status: http.StatusUnauthorized},
		{name: "unknown key", key: "wrong-secret", status: http.StatusUnauthorized},
		{name: "known key", key: "good", status: http.StatusOK},
	}

	h := MiddlewareAuthentication([]string{"good"}, []string{"alice"})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.key != "" {
				req.Header.Set(ApiKeyHeader, tt.key)
			}

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d", rec.Code, tt.status)
			}
			if tt.key != "" && strings.Contains(rec.Body.String(), tt.key) {
				t.Fatalf("body echoes the key: %q", rec.Body.String())
			}
		})
	}
}

func TestMiddlewareAuthenticationPanicsOnMismatch(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic")
		}
	}()

	MiddlewareAuthentication([]string{"a", "b"}, []string{"alice"})
}

func TestRedactURL(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"https://example.com/a/b?token=secret#frag", "https://example.com/a/b"},
		{"https://user:pass@example.com/x", "https://example.com/x"},
		{"https://example.com/plain", "https://example.com/plain"},
		{"://bad", "invalid URL"},
	}

	for _, tt := range tests {
		if got := redactURL(tt.in); got != tt.want {
			t.Errorf("redactURL(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
