package cli

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestBanner(t *testing.T) {
	want := "" +
		" █████╗ ███╗   ██╗████████╗███████╗\n" +
		"██╔══██╗████╗  ██║╚══██╔══╝██╔════╝\n" +
		"███████║██╔██╗ ██║   ██║   ███████╗\n" +
		"██╔══██║██║╚██╗██║   ██║   ╚════██║\n" +
		"██║  ██║██║ ╚████║   ██║   ███████║\n" +
		"╚═╝  ╚═╝╚═╝  ╚═══╝   ╚═╝   ╚══════╝\n" +
		"         powered by ProbeLab · 2026\n" +
		"\n"
	require.Equal(t, want, banner("ants", 2026))
}

func TestBannerUnknownCharacter(t *testing.T) {
	require.Equal(t, "nä\npowered by ProbeLab · 2026\n\n", banner("nä", 2026))
}

func TestRootCommandBanner(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		env        map[string]string
		wantBanner bool
	}{
		{name: "default", args: []string{"app"}, wantBanner: true},
		{name: "flag", args: []string{"app", "--no-banner"}},
		{name: "env", args: []string{"app"}, env: map[string]string{"APP_NO_BANNER": "true"}},
		{name: "completion", args: []string{"app", "--generate-shell-completion"}},
		{name: "help", args: []string{"app", "--help"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			var stderr bytes.Buffer
			cmd := &cli.Command{
				Name:                  "app",
				ErrWriter:             &stderr,
				Writer:                &bytes.Buffer{},
				EnableShellCompletion: true,
				Action:                func(context.Context, *cli.Command) error { return nil },
			}
			root, _ := NewRootCommand(cmd)

			require.NoError(t, root.RunWithContextAndArgs(context.Background(), tt.args))
			if tt.wantBanner {
				require.Contains(t, stderr.String(), "powered by ProbeLab")
			} else {
				require.Empty(t, stderr.String())
			}
		})
	}
}
