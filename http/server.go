package http

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// ListenAndServe binds srv.Addr, serves until ctx is done, and then shuts
// the server down, giving in-flight requests up to grace to finish. A bind
// failure is returned before anything is served. A server that stopped
// because of the shutdown returns nil.
func ListenAndServe(ctx context.Context, srv *http.Server, grace time.Duration) error {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", srv.Addr, err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()

	shutdownErr := srv.Shutdown(shutdownCtx)
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	if shutdownErr != nil {
		return fmt.Errorf("shutdown: %w", shutdownErr)
	}
	return nil
}
