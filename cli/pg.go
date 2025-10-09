package cli

import (
	"github.com/urfave/cli/v3"

	"github.com/probe-lab/go-commons/db"
)

func PostgresFlags(envPrefix string, cfg *db.PostgresConfig) []cli.Flag {
	envPrefix = buildEnvPrefix(envPrefix)
	return append(postgresBaseFlags(envPrefix, cfg.BaseConfig),
		&cli.StringFlag{
			Name:        "postgres.database",
			Usage:       "The Postgres database name to connect to",
			Sources:     cli.EnvVars(envPrefix + "POSTGRES_DATABASE"),
			Value:       cfg.Database,
			Destination: &cfg.Database,
			Category:    flagCategoryDatabase,
		},
	)
}

func PostgresMultiFlags(envPrefix string, cfg *db.PostgresMultiConfig) []cli.Flag {
	envPrefix = buildEnvPrefix(envPrefix)
	return append(postgresBaseFlags(envPrefix, cfg.BaseConfig),
		&cli.StringSliceFlag{
			Name:        "postgres.databases",
			Usage:       "A list of postgres databases that the user has access to. Separate multiple databases with commas.",
			Sources:     cli.EnvVars(envPrefix + "POSTGRES_DATABASES"),
			Value:       cfg.Databases,
			Destination: &cfg.Databases,
			Category:    flagCategoryDatabase,
		},
	)
}

func postgresBaseFlags(envPrefix string, cfg *db.PostgresBaseConfig) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "postgres.host",
			Usage:       "The address where Postgres is hosted",
			Sources:     cli.EnvVars(envPrefix + "POSTGRES_HOST"),
			Value:       cfg.Host,
			Destination: &cfg.Host,
			Category:    flagCategoryDatabase,
		},
		&cli.IntFlag{
			Name:        "postgres.port",
			Usage:       "Port at which the Postgres database is accessible",
			Sources:     cli.EnvVars(envPrefix + "POSTGRES_PORT"),
			Value:       cfg.Port,
			Destination: &cfg.Port,
			Category:    flagCategoryDatabase,
		},
		&cli.StringFlag{
			Name:        "postgres.user",
			Usage:       "The Postgres user that has the right privileges",
			Sources:     cli.EnvVars(envPrefix + "POSTGRES_USER"),
			Value:       cfg.User,
			Destination: &cfg.User,
			Category:    flagCategoryDatabase,
		},
		&cli.StringFlag{
			Name:        "postgres.password",
			Usage:       "The password for the Postgres user",
			Sources:     cli.EnvVars(envPrefix + "POSTGRES_PASSWORD"),
			Value:       cfg.Pass,
			Destination: &cfg.Pass,
			Category:    flagCategoryDatabase,
		},
		&cli.StringFlag{
			Name:        "postgres.sslmode",
			Usage:       "Which SSL mode should be used to connect to Postgres",
			Sources:     cli.EnvVars(envPrefix + "POSTGRES_SSLMODE"),
			Value:       cfg.SSLMode,
			Destination: &cfg.SSLMode,
			Category:    flagCategoryDatabase,
		},
	}
}
