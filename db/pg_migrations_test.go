package db

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"testing/fstest"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostgresMigrationsConfig_Apply runs against a real server. Set
// GO_COMMONS_TEST_POSTGRES_DSN, e.g. "host=localhost port=5432 user=postgres
// dbname=postgres sslmode=disable", to run it.
func TestPostgresMigrationsConfig_Apply(t *testing.T) {
	dsn := os.Getenv("GO_COMMONS_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("GO_COMMONS_TEST_POSTGRES_DSN not set")
	}

	ctx := context.Background()
	handle, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })

	cleanup := func() {
		_, _ = handle.ExecContext(ctx, "DROP TABLE IF EXISTS gc_test_things, gc_test_migrations")
	}
	cleanup()
	t.Cleanup(cleanup)

	migrations := fstest.MapFS{
		"sql/000001_things.up.sql":   {Data: []byte("CREATE TABLE gc_test_things (id INT);")},
		"sql/000001_things.down.sql": {Data: []byte("DROP TABLE gc_test_things;")},
	}

	cfg := DefaultPostgresMigrationsConfig()
	cfg.MigrationsTable = "gc_test_migrations"
	cfg.Dir = "sql"

	require.NoError(t, cfg.Apply(ctx, handle, migrations))

	// a second run is a no-op
	require.NoError(t, cfg.Apply(ctx, handle, migrations))

	var version int
	require.NoError(t, handle.QueryRowContext(ctx, "SELECT version FROM gc_test_migrations").Scan(&version))
	assert.Equal(t, 1, version)

	// the handle stays usable after Apply returned its connection
	_, err = handle.ExecContext(ctx, "INSERT INTO gc_test_things VALUES (1)")
	require.NoError(t, err)
}
