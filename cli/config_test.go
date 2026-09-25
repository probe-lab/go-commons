package cli

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func redactedJSON(t *testing.T, cfg any) map[string]any {
	t.Helper()

	dat, err := json.Marshal(cfg)
	require.NoError(t, err)

	var generic any
	require.NoError(t, json.Unmarshal(dat, &generic))
	redactSecrets(generic)

	m, ok := generic.(map[string]any)
	require.True(t, ok, "expected map, got %T", generic)
	return m
}

func TestMaskSecret(t *testing.T) {
	cases := map[string]string{
		"":           "***",
		"a":          "***",
		"ab":         "***",
		"abc":        "ab***",
		"mypassword": "my***",
	}
	for in, want := range cases {
		require.Equal(t, want, maskSecret(in), "maskSecret(%q)", in)
	}
}

func TestRedactSecrets(t *testing.T) {
	type nested struct {
		Secret string `json:"secret"`
	}
	cfg := struct {
		User     string `json:"user"`
		Password string `json:"password"`
		Timeout  int    `json:"passTimeout"`
		DSN      string `json:"dsn"`
		Nested   nested `json:"nested"`
	}{
		User:     "alice",
		Password: "mypassword",
		Timeout:  5,
		DSN:      "postgres://alice:mypassword@localhost:5432/db",
		Nested:   nested{Secret: "topsecret"},
	}

	m := redactedJSON(t, cfg)

	require.Equal(t, "alice", m["user"])
	require.Equal(t, "my***", m["password"])
	require.Equal(t, "***", m["passTimeout"], "non-string secrets are hidden whole")
	require.Equal(t, "postgres://alice:my***@localhost:5432/db", m["dsn"])
	require.Equal(t, "to***", m["nested"].(map[string]any)["secret"])
}

func TestRedactEnvVar(t *testing.T) {
	cases := map[string]string{
		"HOME=/home/alice":                       "HOME=/home/alice",
		"NOEQUALS":                               "NOEQUALS",
		"PG_PASSWORD=mypassword":                 "PG_PASSWORD=my***",
		"PG_PASSWORD=":                           "PG_PASSWORD=***",
		"AWS_SECRET_ACCESS_KEY=abc=def":          "AWS_SECRET_ACCESS_KEY=ab***",
		"DSN=postgres://alice:mypassword@db/app": "DSN=postgres://alice:my***@db/app",
	}
	for in, want := range cases {
		require.Equal(t, want, redactEnvVar(in), "redactEnvVar(%q)", in)
	}
}
