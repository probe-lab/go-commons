package cli

import (
	"encoding/json"
	"testing"
)

func redactedJSON(t *testing.T, cfg any) map[string]any {
	t.Helper()

	dat, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var generic any
	if err := json.Unmarshal(dat, &generic); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	redactSecrets(generic)

	m, ok := generic.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", generic)
	}
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
		if got := maskSecret(in); got != want {
			t.Errorf("maskSecret(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRedactSecrets(t *testing.T) {
	type nested struct {
		Secret string `json:"secret"`
	}
	cfg := struct {
		User     string `json:"user"`
		Password string `json:"password"`
		DSN      string `json:"dsn"`
		Nested   nested `json:"nested"`
	}{
		User:     "alice",
		Password: "mypassword",
		DSN:      "postgres://alice:mypassword@localhost:5432/db",
		Nested:   nested{Secret: "topsecret"},
	}

	m := redactedJSON(t, cfg)

	if m["user"] != "alice" {
		t.Errorf("user should be untouched, got %v", m["user"])
	}
	if m["password"] != "my***" {
		t.Errorf("password not masked, got %v", m["password"])
	}
	if got := m["dsn"]; got != "postgres://alice:my***@localhost:5432/db" {
		t.Errorf("dsn credentials not masked, got %v", got)
	}
	if nm := m["nested"].(map[string]any); nm["secret"] != "to***" {
		t.Errorf("nested secret not masked, got %v", nm["secret"])
	}
}
