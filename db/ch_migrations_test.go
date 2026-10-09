package db

import (
	"context"
	"os"
	"testing"
	"testing/fstest"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClickHouseMigrationsConfig_Apply runs against a real server. Set
// GO_COMMONS_TEST_CLICKHOUSE_ADDR (host:port of the native protocol) and
// optionally GO_COMMONS_TEST_CLICKHOUSE_USER, _PASSWORD, and _DATABASE.
func TestClickHouseMigrationsConfig_Apply(t *testing.T) {
	addr := os.Getenv("GO_COMMONS_TEST_CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("GO_COMMONS_TEST_CLICKHOUSE_ADDR not set")
	}

	opt := &clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: os.Getenv("GO_COMMONS_TEST_CLICKHOUSE_DATABASE"),
			Username: os.Getenv("GO_COMMONS_TEST_CLICKHOUSE_USER"),
			Password: os.Getenv("GO_COMMONS_TEST_CLICKHOUSE_PASSWORD"),
		},
	}

	ctx := context.Background()
	conn, err := clickhouse.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	cleanup := func() {
		_ = conn.Exec(ctx, "DROP TABLE IF EXISTS gc_test_things")
		_ = conn.Exec(ctx, "DROP TABLE IF EXISTS gc_test_migrations")
	}
	cleanup()
	t.Cleanup(cleanup)

	// Written for a cluster; Apply strips "Replicated" for a single server.
	migrations := fstest.MapFS{
		"sql/000001_things.up.sql":   {Data: []byte("CREATE TABLE gc_test_things (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id;")},
		"sql/000001_things.down.sql": {Data: []byte("DROP TABLE gc_test_things;")},
	}

	cfg := DefaultClickHouseMigrationsConfig()
	cfg.MigrationsTable = "gc_test_migrations"
	cfg.Dir = "sql"

	require.NoError(t, cfg.Apply(opt, migrations))
	require.NoError(t, cfg.Apply(opt, migrations))

	var engine string
	require.NoError(t, conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'gc_test_things'").Scan(&engine))
	assert.Equal(t, "MergeTree", engine)
}
