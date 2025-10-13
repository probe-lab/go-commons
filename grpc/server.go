package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type ServerConfig struct {
	Listener net.Listener
	Host     string
	Port     int
	LogOpts  []logging.Option
}

func (cfg *ServerConfig) Validate() error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	if cfg.Listener != nil {
		if cfg.Host != "" {
			return fmt.Errorf("listener and host cannot both be set")
		}

		if cfg.Port != 0 {
			return fmt.Errorf("listener and port cannot both be set")
		}

		return nil
	}

	if cfg.Host == "" {
		return fmt.Errorf("no listener provided and host is empty")
	}

	if cfg.Port < 0 {
		return fmt.Errorf("no listener provided and port is negative")
	}

	return nil
}

type Server struct {
	cfg    *ServerConfig
	server *grpc.Server
	health *health.Server
}

// NewServer creates and returns a new gRPC Server instance.
// It sets up a panic recovery mechanism that limits the rate of panic logs.
// The server is configured with OpenTelemetry for metrics.
func NewServer(cfg *ServerConfig) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	loggingOpts := append([]logging.Option{
		logging.WithLogOnEvents(logging.FinishCall),
		logging.WithDisableLoggingFields(logging.ServiceFieldKey, logging.ComponentFieldKey, logging.MethodTypeFieldKey),
	}, cfg.LogOpts...)

	// Create a new gRPC server
	server := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			logging.UnaryServerInterceptor(loggerInterceptor(), loggingOpts...),
			recovery.UnaryServerInterceptor(recoverInterceptor()),
		),
	)

	healthcheck := health.NewServer()
	healthgrpc.RegisterHealthServer(server, healthcheck)

	return &Server{
		server: server,
		cfg:    cfg,
		health: healthcheck,
	}, nil
}

func (s *Server) SetServingStatus(service string, servingStatus healthv1.HealthCheckResponse_ServingStatus) {
	slog.Debug("Setting health status", "service", service, "status", servingStatus)
	s.health.SetServingStatus(service, servingStatus)
}

func (s *Server) ListenAndServe() error {
	lis, err := s.listener()
	if err != nil {
		return fmt.Errorf("new listener: %w", err)
	}

	slog.Info("Starting gRPC server", "addr", lis.Addr())
	defer slog.Info("Stopped gRPC server", "addr", lis.Addr())

	s.health.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
	defer s.health.SetServingStatus("", healthgrpc.HealthCheckResponse_NOT_SERVING)

	return s.server.Serve(lis)
}

func (s *Server) Shutdown() {
	slog.Info("Shutting down gRPC server")
	s.health.Shutdown()
	s.server.GracefulStop()
}

// BindCtx binds the given context to the server's lifecycle. Cancelling the
// context shuts down the server. You must call the returned shutdown
// function in order to not leak resources.
func (s *Server) BindCtx(ctx context.Context) func() {
	ctx, cancel := context.WithCancel(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		s.Shutdown()
	}()

	shutdownFn := func() {
		cancel()
		<-done
	}

	return shutdownFn
}

var _ grpc.ServiceRegistrar = (*Server)(nil)

func (s *Server) RegisterService(desc *grpc.ServiceDesc, impl any) {
	s.server.RegisterService(desc, impl)
}

func (s *Server) listener() (net.Listener, error) {
	if s.cfg.Listener != nil {
		return s.cfg.Listener, nil
	}

	addr := net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port))
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tcp listen on %s: %w", addr, err)
	}

	return lis, nil
}

func loggerInterceptor() logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		slog.Log(ctx, slog.Level(lvl), msg, fields...)
	})
}

func recoverInterceptor() recovery.Option {
	panicsCounter, err := otel.GetMeterProvider().Meter("grpc.server").Int64Counter("grpc_req_panics_recovered_total", metric.WithDescription("Total number of gRPC requests recovered from internal panic."))
	if err != nil {
		panic(err)
	}

	// limit panic logs to 1 per second to not overwhelm the logging system
	r := rate.NewLimiter(rate.Every(time.Second), 1)

	handler := recovery.WithRecoveryHandlerContext(func(ctx context.Context, p any) (err error) {
		panicsCounter.Add(ctx, 1)
		if r.Allow() {
			slog.Error("Recovered from panic", "panic", p, "stack", debug.Stack())
		}
		return status.Errorf(codes.Internal, "%s", p)
	})

	return handler
}
