package cli

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestNewRootCommand(t *testing.T) {
	var ran bool
	cmd := &cli.Command{
		Name:    "app",
		Version: "1.2.3",
		Action: func(context.Context, *cli.Command) error {
			ran = true
			return nil
		},
	}

	root, cfg := NewRootCommand(cmd)
	require.Equal(t, "APP_", cfg.EnvPrefix)
	require.True(t, len(cmd.Version) >= len("1.2.3"), "the commit is appended to the version")
	require.Equal(t, cmd.Version, cfg.Metrics.Version, "the metrics resource carries the command version")
	require.Equal(t, cmd.Version, cfg.Trace.Version, "the trace resource carries the command version")

	err := root.RunWithContextAndArgs(context.Background(), []string{"app", "--log.level", "debug"})
	require.NoError(t, err)
	require.True(t, ran)
	require.Equal(t, "debug", cfg.Log.Level)
}

func TestRedactEnvVar(t *testing.T) {
	require.Equal(t, "HOME=/root", redactEnvVar("HOME=/root"))
	require.Equal(t, "DB_PASSWORD=*****", redactEnvVar("DB_PASSWORD=secret"))
	require.Equal(t, "DB_PASSWORD=*****", redactEnvVar("DB_PASSWORD=with=equals=="))
	require.Equal(t, "DB_PASSWORD=", redactEnvVar("DB_PASSWORD="))
	require.Equal(t, "NOVALUE", redactEnvVar("NOVALUE"))
}
