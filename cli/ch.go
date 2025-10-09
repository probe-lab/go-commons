package cli

import (
	"github.com/probe-lab/go-commons/db"
	"github.com/urfave/cli/v3"
)

// ClickHouseFlags constructs a slice of [cli.Flag] instances used for configuring
// a connection to a ClickHouse database. This function incorporates flags for
// the base configuration (including host, port, user, password, and SSL
// settings) and adds an additional flag specific to the database configuration,
// allowing for the specification of a database name. The environment variable
// prefixes provided with 'envPrefix' enable configuration through environment
// variables.
//
// In some settings a service will only connect to a single ClickHouse database
// and in some other settings it will require connections to multiple. Hence
// the split between the [ClickHouseConfig] and [ClickHouseMultiConfig] and the
// abstraction of a [ClickHouseBaseConfig].
func ClickHouseFlags(envPrefix string, cfg *db.ClickHouseConfig) []cli.Flag {
	envPrefix = buildEnvPrefix(envPrefix)
	return append(ClickHouseBaseFlags(envPrefix, cfg.BaseConfig),
		&cli.StringFlag{
			Name:        "clickhouse.database",
			Usage:       "The ClickHouse database name to connect to",
			Sources:     cli.EnvVars(envPrefix + "CLICKHOUSE_DATABASE"),
			Value:       cfg.Database,
			Destination: &cfg.Database,
			Category:    flagCategoryDatabase,
		},
	)
}

// ClickHouseMultiFlags generates a slice of [cli.Flag] for configuring multiple
// ClickHouse databases. It integrates the base flags with additional flags for
// setting up multiple database access, utilizing environment variables prefixed
// by envPrefix for dynamic configuration.
func ClickHouseMultiFlags(envPrefix string, cfg *db.ClickHouseMultiConfig) []cli.Flag {
	envPrefix = buildEnvPrefix(envPrefix)
	return append(ClickHouseBaseFlags(envPrefix, cfg.BaseConfig),
		&cli.StringSliceFlag{
			Name:        "clickhouse.databases",
			Usage:       "A list of clickhouse databases that the user has access to. Separate multiple databases with commas.",
			Sources:     cli.EnvVars(envPrefix + "CLICKHOUSE_DATABASES"),
			Value:       cfg.Databases,
			Destination: &cfg.Databases,
			Category:    flagCategoryDatabase,
		},
	)
}

// ClickHouseBaseFlags generates a slice of cli.Flag for configuring the basic
// connection settings to a ClickHouse server through a command-line interface.
// These flags allow users to specify the host, port, user, password, and SSL
// configuration for connecting to a ClickHouse database. Each flag can be
// customized with environment variables that are prefixed with 'envPrefix'.
//
// This function works in conjunction with other configuration functions like
// [ClickHouseFlags] and [ClickHouseMultiFlags], which extend the base
// configuration to include database-specific settings. Together, they allow
// users to specify a comprehensive set of options for establishing connections
// to one or more ClickHouse databases.
func ClickHouseBaseFlags(envPrefix string, cfg *db.ClickHouseBaseConfig) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "clickhouse.host",
			Usage:       "The address where ClickHouse is hosted",
			Sources:     cli.EnvVars(envPrefix + "CLICKHOUSE_HOST"),
			Value:       cfg.Host,
			Destination: &cfg.Host,
			Category:    flagCategoryDatabase,
		},
		&cli.IntFlag{
			Name:        "clickhouse.port",
			Usage:       "Port at which the ClickHouse database is accessible",
			Sources:     cli.EnvVars(envPrefix + "CLICKHOUSE_PORT"),
			Value:       cfg.Port,
			Destination: &cfg.Port,
			Category:    flagCategoryDatabase,
		},
		&cli.StringFlag{
			Name:        "clickhouse.user",
			Usage:       "The ClickHouse user that has the right privileges",
			Sources:     cli.EnvVars(envPrefix + "CLICKHOUSE_USER"),
			Value:       cfg.User,
			Destination: &cfg.User,
			Category:    flagCategoryDatabase,
		},
		&cli.StringFlag{
			Name:        "clickhouse.password",
			Usage:       "The password for the ClickHouse user",
			Sources:     cli.EnvVars(envPrefix + "CLICKHOUSE_PASSWORD"),
			Value:       cfg.Pass,
			Destination: &cfg.Pass,
			Category:    flagCategoryDatabase,
		},
		&cli.BoolFlag{
			Name:        "clickhouse.ssl",
			Usage:       "Whether to use SSL to connect to ClickHouse",
			Sources:     cli.EnvVars(envPrefix + "CLICKHOUSE_SSL"),
			Value:       cfg.SSL,
			Destination: &cfg.SSL,
			Category:    flagCategoryDatabase,
		},
	}
}
