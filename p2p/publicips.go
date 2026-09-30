package p2p

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// FetchPublicIPv4 returns this host's public IPv4 as seen from the internet via
// checkip.amazonaws.com, or "" if it cannot be resolved. It reflects the egress
// IPv4 (e.g. a NAT gateway's EIP, or a directly-assigned public IPv4), so it is
// agnostic to the network topology.
func FetchPublicIPv4(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	slog.Info("resolving public IPv4 via checkip.amazonaws.com")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://checkip.amazonaws.com", nil)
	if err != nil {
		slog.Warn("build public IP request", "err", err)
		return ""
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Warn("fetch public IP", "err", err)
		return ""
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		slog.Warn("read public IP response", "err", err)
		return ""
	}

	ip := strings.TrimSpace(string(body))
	if net.ParseIP(ip).To4() == nil {
		slog.Warn("public IP is not IPv4", "value", ip)
		return ""
	}

	slog.Info("resolved public IPv4", "ip", ip)
	return ip
}
