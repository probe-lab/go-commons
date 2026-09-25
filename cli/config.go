package cli

import (
	"encoding/json"
	"log/slog"
	"net/url"
	"os"
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

// debugPrintEnvVars logs all environment variables at debug level, with the
// same redaction as SafePrintConfig.
func debugPrintEnvVars() {
	slog.Debug("Environment variables:")
	for _, kv := range os.Environ() {
		slog.Debug(redactEnvVar(kv))
	}
}

// redactEnvVar hides the secret in a KEY=VALUE pair. Values may themselves
// contain "=", so the pair is split at the first one only.
func redactEnvVar(kv string) string {
	key, val, ok := strings.Cut(kv, "=")
	if !ok {
		return kv
	}
	return key + "=" + redactString(key, val)
}

// redactSecrets walks decoded JSON and hides secrets in place.
func redactSecrets(v any) {
	switch t := v.(type) {
	case map[string]any:
		for key, val := range t {
			if s, ok := val.(string); ok {
				t[key] = redactString(key, s)
			} else if isSecretKey(key) {
				t[key] = "***"
			} else {
				redactSecrets(val)
			}
		}
	case []any:
		for _, val := range t {
			redactSecrets(val)
		}
	}
}

// redactString returns val with its secret hidden: the whole value when key
// names a secret, otherwise only the password of a URL embedded in it.
func redactString(key, val string) string {
	if isSecretKey(key) {
		return maskSecret(val)
	}
	return redactURLCredentials(val)
}

// isSecretKey reports whether a field or variable name suggests a secret.
func isSecretKey(key string) bool {
	lower := strings.ToLower(key)
	return strings.Contains(lower, "pass") || strings.Contains(lower, "secret")
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
