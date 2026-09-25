package cli

import (
	"encoding/json"
	"log/slog"
	"net/url"
	"strings"
)

// SafePrintConfig JSON-formats any command configuration and logs it, redacting
// secret fields (passwords, secrets and URL-embedded credentials) beforehand.
func SafePrintConfig(tag string, cfg any) {
	dat, err := json.Marshal(cfg)
	if err != nil {
		slog.Warn("failed marshalling config", "tag", tag, "err", err.Error())
		return
	}

	var generic any
	if err := json.Unmarshal(dat, &generic); err != nil {
		slog.Warn("failed unmarshalling config", "tag", tag, "err", err.Error())
		return
	}
	redactSecrets(generic)

	dat, err = json.MarshalIndent(generic, "", "  ")
	if err != nil {
		slog.Warn("failed marshalling config", "tag", tag, "err", err.Error())
		return
	}

	slog.Info("config", "tag", tag, "config", string(dat))
}

func redactSecrets(v any) {
	switch t := v.(type) {
	case map[string]any:
		for key, val := range t {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "pass") || strings.Contains(lower, "secret") {
				if s, ok := val.(string); ok {
					t[key] = maskSecret(s)
				} else {
					t[key] = "***"
				}
				continue
			}
			if s, ok := val.(string); ok {
				t[key] = redactURLCredentials(s)
				continue
			}
			redactSecrets(val)
		}
	case []any:
		for _, val := range t {
			redactSecrets(val)
		}
	}
}

func maskSecret(s string) string {
	if len(s) <= 2 {
		return "***"
	}
	return s[:2] + "***"
}

func redactURLCredentials(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	pwd, hasPwd := u.User.Password()
	if !hasPwd {
		return raw
	}

	// rebuilt manually so maskSecret's '*' isn't percent-encoded to "%2A"
	username := u.User.Username()
	u.User = nil
	rest := u.String()
	i := strings.Index(rest, "://")
	if i < 0 {
		return raw
	}
	insert := i + len("://")
	return rest[:insert] + username + ":" + maskSecret(pwd) + "@" + rest[insert:]
}
