package grpc

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

func TestServer_lifecycle(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelError)
	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { assert.NoError(t, lis.Close()) })

	cfg := &ServerConfig{
		Listener: lis,
	}

	s, err := NewServer(cfg)
	require.NoError(t, err)
	t.Cleanup(s.Shutdown)

	shutdownFn := s.BindCtx(context.Background())

	done := make(chan struct{})
	go func() {
		require.NoError(t, s.ListenAndServe())
		close(done)
	}()
	// initialize in-memory gRPC connection
	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })

	client := healthgrpc.NewHealthClient(conn)

	resp, err := client.Check(context.Background(), &healthgrpc.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthgrpc.HealthCheckResponse_SERVING, resp.Status)

	shutdownFn()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fail()
	}
}

func TestServer_healthcheck(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelError)

	// initialize in-memory listener for gRPC communication
	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { assert.NoError(t, lis.Close()) })

	cfg := &ServerConfig{
		Listener: lis,
	}

	s, err := NewServer(cfg)
	require.NoError(t, err)
	t.Cleanup(s.Shutdown)

	// initialize in-memory gRPC connection
	conn, err := grpc.NewClient("passthrough://bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })

	go func() { require.NoError(t, s.ListenAndServe()) }()

	client := healthgrpc.NewHealthClient(conn)

	resp, err := client.Check(context.Background(), &healthgrpc.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthgrpc.HealthCheckResponse_SERVING, resp.Status)

	s.SetServingStatus("", healthgrpc.HealthCheckResponse_NOT_SERVING)
	resp, err = client.Check(context.Background(), &healthgrpc.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthgrpc.HealthCheckResponse_NOT_SERVING, resp.Status)
}

func TestServerConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *ServerConfig
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "nil",
			cfg:     nil,
			wantErr: assert.Error,
		},
		{
			name: "listener with host",
			cfg: &ServerConfig{
				Listener: &bufconn.Listener{},
				Host:     "localhost",
				Port:     8080,
			},
			wantErr: assert.Error,
		},
		{
			name: "listener with port",
			cfg: &ServerConfig{
				Listener: &bufconn.Listener{},
				Host:     "",
				Port:     8080,
			},
			wantErr: assert.Error,
		},
		{
			name: "no host",
			cfg: &ServerConfig{
				Listener: nil,
				Host:     "",
				Port:     8080,
			},
			wantErr: assert.Error,
		},
		{
			name: "negative port",
			cfg: &ServerConfig{
				Listener: nil,
				Host:     "localhost",
				Port:     -1,
			},
			wantErr: assert.Error,
		},
		{
			name: "0 port allowed",
			cfg: &ServerConfig{
				Host: "localhost",
				Port: 0,
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, tt.cfg.Validate(), "Validate()")
		})
	}
}
