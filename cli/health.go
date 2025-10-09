package cli

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/urfave/cli/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

func NewHealthCommand() *cli.Command {
	return &cli.Command{
		Name:   "health",
		Usage:  "Checks the health of the provided endpoint",
		Action: healthAction,
	}
}

func healthAction(ctx context.Context, c *cli.Command) error {
	addr := c.Args().First()
	if addr == "" {
		addr = "localhost:8080"
	}

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(addr, options...)
	if err != nil {
		return fmt.Errorf("new gRPC health client %s: %v", addr, err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			slog.Warn("Failed closing gRPC health client", "addr", addr, "err", err)
		}
	}()

	client := healthgrpc.NewHealthClient(conn)

	resp, err := client.Check(ctx, &healthgrpc.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("check health: %v", err)
	}

	switch resp.GetStatus() {
	case healthgrpc.HealthCheckResponse_SERVING:
		return nil
	case healthgrpc.HealthCheckResponse_NOT_SERVING:
		return fmt.Errorf("health check status is not serving")
	case healthgrpc.HealthCheckResponse_SERVICE_UNKNOWN:
		return fmt.Errorf("health check service unknown")
	default:
		return fmt.Errorf("unknown health check status")
	}
}
