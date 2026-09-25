package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newClient(t *testing.T, cfg *ClientConfig) *Client {
	t.Helper()
	c, err := NewClient(cfg)
	require.NoError(t, err)
	return c
}

func TestClientConfigValidate(t *testing.T) {
	require.NoError(t, DefaultClientConfig().Validate())

	var nilCfg *ClientConfig
	require.Error(t, nilCfg.Validate())

	for name, mutate := range map[string]func(*ClientConfig){
		"empty user agent":    func(c *ClientConfig) { c.UserAgent = "" },
		"zero timeout":        func(c *ClientConfig) { c.Timeout = 0 },
		"zero max body":       func(c *ClientConfig) { c.MaxBody = 0 },
		"negative redirects":  func(c *ClientConfig) { c.MaxRedirects = -1 },
		"negative rate limit": func(c *ClientConfig) { c.RPS = -1 },
	} {
		cfg := DefaultClientConfig()
		mutate(cfg)
		require.Error(t, cfg.Validate(), name)
		_, err := NewClient(cfg)
		require.Error(t, err, name)
	}
}

func TestClientSendsUserAgentAndDecodesJSON(t *testing.T) {
	var gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte(`{"name":"x"}`))
	}))
	defer srv.Close()

	c := newClient(t, DefaultClientConfig())
	var v struct{ Name string }
	res, err := c.GetJSON(context.Background(), srv.URL, &v)
	require.NoError(t, err)
	require.Equal(t, DefaultUserAgent, gotUA)
	require.Equal(t, "x", v.Name)
	require.Equal(t, "text/plain", res.ContentType)
	require.Equal(t, http.StatusOK, res.StatusCode)
}

func TestClientStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	res, err := newClient(t, DefaultClientConfig()).Get(context.Background(), srv.URL, "")
	require.Error(t, err)
	require.True(t, IsNotFound(err))
	status, ct, ok := AsStatus(err)
	require.True(t, ok)
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, "text/html", ct)
	require.NotNil(t, res, "the response is returned alongside the error")
	require.Equal(t, http.StatusNotFound, res.StatusCode)

	require.False(t, (&StatusError{StatusCode: http.StatusInternalServerError}).NotFound())
	require.True(t, (&StatusError{StatusCode: http.StatusGone}).NotFound())
}

func TestClientBodyCap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 100)))
	}))
	defer srv.Close()

	cfg := DefaultClientConfig()
	cfg.MaxBody = 50
	_, err := newClient(t, cfg).Get(context.Background(), srv.URL, "")
	require.ErrorContains(t, err, "exceeds 50 bytes")
}

func TestClientRedirectCap(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, r.URL.Path+"x", http.StatusFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := DefaultClientConfig()
	cfg.MaxRedirects = 2
	_, err := newClient(t, cfg).Get(context.Background(), srv.URL+"/", "")
	require.ErrorContains(t, err, "stopped after 2 redirects")
}

func TestClientDoSendsBodyAndHeaders(t *testing.T) {
	var gotMethod, gotBody, gotCT, gotExtra string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		b := make([]byte, 16)
		n, _ := r.Body.Read(b)
		gotBody = string(b[:n])
		gotCT = r.Header.Get("Content-Type")
		gotExtra = r.Header.Get("X-Extra")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	res, err := newClient(t, DefaultClientConfig()).Do(context.Background(), ClientRequest{
		Method:      http.MethodPost,
		URL:         srv.URL,
		Body:        []byte("hello"),
		ContentType: "text/plain",
		Header:      http.Header{"X-Extra": {"1"}},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, res.StatusCode)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "hello", gotBody)
	require.Equal(t, "text/plain", gotCT)
	require.Equal(t, "1", gotExtra)
}

func TestClientWaitHonoursContext(t *testing.T) {
	cfg := DefaultClientConfig()
	cfg.RPS = 0.001
	c := newClient(t, cfg)
	require.NoError(t, c.Wait(context.Background()), "the first request is admitted by the burst")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, c.Wait(ctx), "the second one has to wait and sees the cancelled context")
}

func TestClientRetriesTransientFailures(t *testing.T) {
	retryBase = time.Millisecond
	t.Cleanup(func() { retryBase = time.Second })

	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(`{"name":"x"}`))
	}))
	defer srv.Close()

	c := newClient(t, DefaultClientConfig())

	var v struct{ Name string }
	_, err := c.DoJSON(context.Background(), ClientRequest{URL: srv.URL, Retries: 3}, &v)
	require.NoError(t, err)
	require.Equal(t, "x", v.Name)
	require.Equal(t, 3, calls)

	// A 4xx is not retried.
	calls = 0
	srv404 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv404.Close()

	_, err = c.Do(context.Background(), ClientRequest{URL: srv404.URL, Retries: 3})
	require.True(t, IsNotFound(err))
	require.Equal(t, 1, calls)

	// Without Retries the first 5xx is returned.
	calls = 0
	_, err = c.Do(context.Background(), ClientRequest{URL: srv.URL})
	require.Error(t, err)
	require.Equal(t, 1, calls)
}
