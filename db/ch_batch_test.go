package db

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time interface checks.
var _ driver.Conn = (*mockConn)(nil)
var _ driver.Batch = (*mockBatch)(nil)
var _ batchLifecycle = (*BatchInserter[testRow])(nil)

// mockBatch implements driver.Batch for testing.
type mockBatch struct {
	appended  []any
	appendErr error
	sendErr   error
	// onSend is called when Send succeeds, useful for signaling tests.
	onSend func()
}

func (m *mockBatch) Abort() error { return nil }
func (m *mockBatch) Append(v ...any) error { return nil }
func (m *mockBatch) AppendStruct(v any) error {
	if m.appendErr != nil {
		return m.appendErr
	}
	m.appended = append(m.appended, v)
	return nil
}
func (m *mockBatch) Column(int) driver.BatchColumn       { return nil }
func (m *mockBatch) Flush() error                        { return nil }
func (m *mockBatch) IsSent() bool                        { return false }
func (m *mockBatch) Rows() int                           { return len(m.appended) }
func (m *mockBatch) Columns() []column.Interface         { return nil }
func (m *mockBatch) Close() error                        { return nil }
func (m *mockBatch) Send() error {
	if m.sendErr != nil {
		return m.sendErr
	}
	if m.onSend != nil {
		m.onSend()
	}
	return nil
}

// mockConn implements driver.Conn for testing.
type mockConn struct {
	prepareErr   error
	batch        *mockBatch
	prepareCalls int
}

func (m *mockConn) PrepareBatch(_ context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	m.prepareCalls++
	if m.prepareErr != nil {
		return nil, m.prepareErr
	}
	return m.batch, nil
}
func (m *mockConn) Contributors() []string                                           { return nil }
func (m *mockConn) ServerVersion() (*driver.ServerVersion, error)                    { return nil, nil }
func (m *mockConn) Select(_ context.Context, _ any, _ string, _ ...any) error        { return nil }
func (m *mockConn) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) { return nil, nil }
func (m *mockConn) QueryRow(_ context.Context, _ string, _ ...any) driver.Row        { return nil }
func (m *mockConn) Exec(_ context.Context, _ string, _ ...any) error                 { return nil }
func (m *mockConn) AsyncInsert(_ context.Context, _ string, _ bool, _ ...any) error  { return nil }
func (m *mockConn) Ping(_ context.Context) error                                      { return nil }
func (m *mockConn) Stats() driver.Stats                                               { return driver.Stats{} }
func (m *mockConn) Close() error                                                      { return nil }

// testRow is a minimal struct used as the generic type T in tests.
type testRow struct {
	Value int
}

func newTestInserter(t *testing.T, conn *mockConn, cfg *BatchInserterConfig[testRow]) *BatchInserter[testRow] {
	t.Helper()
	b, err := NewBatchInserter[testRow](conn, "test_table", cfg)
	require.NoError(t, err)
	return b
}

func TestBatchInserterConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfgFn   func() *BatchInserterConfig[testRow]
		wantErr bool
	}{
		{
			name:    "valid",
			cfgFn:   DefaultBatchInserterConfig[testRow],
			wantErr: false,
		},
		{
			name:    "nil",
			cfgFn:   func() *BatchInserterConfig[testRow] { return nil },
			wantErr: true,
		},
		{
			name: "zero max batch size",
			cfgFn: func() *BatchInserterConfig[testRow] {
				cfg := DefaultBatchInserterConfig[testRow]()
				cfg.MaxBatchSize = 0
				return cfg
			},
			wantErr: true,
		},
		{
			name: "negative max batch size",
			cfgFn: func() *BatchInserterConfig[testRow] {
				cfg := DefaultBatchInserterConfig[testRow]()
				cfg.MaxBatchSize = -1
				return cfg
			},
			wantErr: true,
		},
		{
			name: "zero flush interval",
			cfgFn: func() *BatchInserterConfig[testRow] {
				cfg := DefaultBatchInserterConfig[testRow]()
				cfg.FlushInterval = 0
				return cfg
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfgFn().Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewBatchInserter(t *testing.T) {
	validConn := &mockConn{batch: &mockBatch{}}
	validCfg := DefaultBatchInserterConfig[testRow]()

	t.Run("nil conn", func(t *testing.T) {
		_, err := NewBatchInserter[testRow](nil, "t", validCfg)
		assert.Error(t, err)
	})
	t.Run("empty table", func(t *testing.T) {
		_, err := NewBatchInserter[testRow](validConn, "", validCfg)
		assert.Error(t, err)
	})
	t.Run("invalid config", func(t *testing.T) {
		cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 0, FlushInterval: time.Second}
		_, err := NewBatchInserter[testRow](validConn, "t", cfg)
		assert.Error(t, err)
	})
	t.Run("valid", func(t *testing.T) {
		b, err := NewBatchInserter[testRow](validConn, "t", validCfg)
		assert.NoError(t, err)
		assert.NotNil(t, b)
	})
}

func TestBatchInserter_Flush_empty(t *testing.T) {
	conn := &mockConn{batch: &mockBatch{}}
	cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 10, FlushInterval: time.Hour}
	b := newTestInserter(t, conn, cfg)
	b.Start(context.Background())

	require.NoError(t, b.Flush(context.Background()))
	assert.Equal(t, 0, conn.prepareCalls)

	require.NoError(t, b.Stop(context.Background()))
}

func TestBatchInserter_Flush_sendsRows(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}
	cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 10, FlushInterval: time.Hour}
	b := newTestInserter(t, conn, cfg)
	b.Start(context.Background())

	for i := range 5 {
		require.NoError(t, b.Add(context.Background(), testRow{Value: i}))
	}
	require.NoError(t, b.Flush(context.Background()))

	assert.Equal(t, 1, conn.prepareCalls)
	assert.Len(t, batch.appended, 5)

	require.NoError(t, b.Stop(context.Background()))
}

func TestBatchInserter_Add_flushOnMaxSize(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}
	cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 3, FlushInterval: time.Hour}
	b := newTestInserter(t, conn, cfg)
	b.Start(context.Background())

	for i := range 3 {
		require.NoError(t, b.Add(context.Background(), testRow{Value: i}))
	}
	// Stop synchronizes: goroutine finishes any in-progress flush before exiting.
	require.NoError(t, b.Stop(context.Background()))

	assert.Equal(t, 1, conn.prepareCalls)
	assert.Len(t, batch.appended, 3)
}

func TestBatchInserter_Stop_finalFlush(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}
	cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 100, FlushInterval: time.Hour}
	b := newTestInserter(t, conn, cfg)
	b.Start(context.Background())

	for i := range 7 {
		require.NoError(t, b.Add(context.Background(), testRow{Value: i}))
	}
	require.NoError(t, b.Stop(context.Background()))

	assert.Equal(t, 1, conn.prepareCalls)
	assert.Len(t, batch.appended, 7)
}

func TestBatchInserter_Stop_idempotent(t *testing.T) {
	conn := &mockConn{batch: &mockBatch{}}
	b := newTestInserter(t, conn, DefaultBatchInserterConfig[testRow]())
	b.Start(context.Background())

	require.NoError(t, b.Stop(context.Background()))
	require.NoError(t, b.Stop(context.Background()))
}

func TestBatchInserter_Add_afterStop(t *testing.T) {
	conn := &mockConn{batch: &mockBatch{}}
	b := newTestInserter(t, conn, DefaultBatchInserterConfig[testRow]())
	b.Start(context.Background())
	require.NoError(t, b.Stop(context.Background()))

	err := b.Add(context.Background(), testRow{Value: 1})
	assert.True(t, errors.Is(err, ErrStopped))
}

func TestBatchInserter_Flush_prepareError(t *testing.T) {
	prepareErr := errors.New("prepare failed")
	conn := &mockConn{prepareErr: prepareErr, batch: &mockBatch{}}
	cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 10, FlushInterval: time.Hour}
	b := newTestInserter(t, conn, cfg)
	b.Start(context.Background())

	require.NoError(t, b.Add(context.Background(), testRow{Value: 1}))

	err := b.Flush(context.Background())
	assert.ErrorContains(t, err, "prepare batch")

	// Buffer was cleared; a second flush is a no-op.
	conn.prepareErr = nil
	require.NoError(t, b.Flush(context.Background()))
	assert.Equal(t, 1, conn.prepareCalls)

	require.NoError(t, b.Stop(context.Background()))
}

func TestBatchInserter_Flush_sendError(t *testing.T) {
	sendErr := errors.New("send failed")
	batch := &mockBatch{sendErr: sendErr}
	conn := &mockConn{batch: batch}
	cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 10, FlushInterval: time.Hour}
	b := newTestInserter(t, conn, cfg)
	b.Start(context.Background())

	require.NoError(t, b.Add(context.Background(), testRow{Value: 1}))

	err := b.Flush(context.Background())
	assert.ErrorContains(t, err, "send batch")

	// Buffer was cleared; a second flush is a no-op.
	batch.sendErr = nil
	require.NoError(t, b.Flush(context.Background()))
	assert.Equal(t, 1, conn.prepareCalls)

	require.NoError(t, b.Stop(context.Background()))
}

func TestBatchInserter_OnDroppedRows(t *testing.T) {
	sendErr := errors.New("send failed")
	batch := &mockBatch{sendErr: sendErr}
	conn := &mockConn{batch: batch}

	var droppedRows []testRow
	var droppedErr error
	cfg := &BatchInserterConfig[testRow]{
		MaxBatchSize:  10,
		FlushInterval: time.Hour,
		OnDroppedRows: func(rows []testRow, err error) {
			droppedRows = rows
			droppedErr = err
		},
	}
	b := newTestInserter(t, conn, cfg)
	b.Start(context.Background())

	require.NoError(t, b.Add(context.Background(), testRow{Value: 42}))
	_ = b.Flush(context.Background())

	assert.ErrorIs(t, droppedErr, sendErr)
	require.Len(t, droppedRows, 1)
	assert.Equal(t, 42, droppedRows[0].Value)

	require.NoError(t, b.Stop(context.Background()))
}

func TestBatchInserter_Start_flushesOnInterval(t *testing.T) {
	sent := make(chan struct{}, 1)
	batch := &mockBatch{onSend: func() { sent <- struct{}{} }}
	conn := &mockConn{batch: batch}
	cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 1000, FlushInterval: 5 * time.Millisecond}
	b := newTestInserter(t, conn, cfg)
	b.Start(context.Background())

	require.NoError(t, b.Add(context.Background(), testRow{Value: 42}))

	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("interval flush did not occur within timeout")
	}

	assert.Len(t, batch.appended, 1)
	v, ok := batch.appended[0].(*testRow)
	require.True(t, ok)
	assert.Equal(t, 42, v.Value)

	require.NoError(t, b.Stop(context.Background()))
}

// TestBatchInserter_rowType verifies that AppendStruct receives a pointer to T.
func TestBatchInserter_rowType(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}
	cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 10, FlushInterval: time.Hour}
	b := newTestInserter(t, conn, cfg)
	b.Start(context.Background())

	require.NoError(t, b.Add(context.Background(), testRow{Value: 99}))
	require.NoError(t, b.Flush(context.Background()))
	require.NoError(t, b.Stop(context.Background()))

	require.Len(t, batch.appended, 1)
	assert.Equal(t, reflect.TypeOf(&testRow{}), reflect.TypeOf(batch.appended[0]))
}

func TestBatchInserterGroup(t *testing.T) {
	batch1, batch2 := &mockBatch{}, &mockBatch{}
	conn1 := &mockConn{batch: batch1}
	conn2 := &mockConn{batch: batch2}
	cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 10, FlushInterval: time.Hour}

	b1 := newTestInserter(t, conn1, cfg)
	b2 := newTestInserter(t, conn2, cfg)

	group := &BatchInserterGroup{}
	group.Add(b1)
	group.Add(b2)
	group.Start(context.Background())

	require.NoError(t, b1.Add(context.Background(), testRow{Value: 1}))
	require.NoError(t, b2.Add(context.Background(), testRow{Value: 2}))

	require.NoError(t, group.Stop(context.Background()))

	assert.Len(t, batch1.appended, 1)
	assert.Len(t, batch2.appended, 1)
}
