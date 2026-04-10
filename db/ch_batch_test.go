package db

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time interface checks.
var (
	_ driver.Conn    = (*mockConn)(nil)
	_ driver.Batch   = (*mockBatch)(nil)
	_ batchLifecycle = (*BatchInserter[testRow])(nil)
)

// mockBatch implements driver.Batch for testing.
type mockBatch struct {
	appended  []any
	appendErr error
	sendErr   error
	// sentSizes records the number of rows in each Send call.
	sentSizes []int
	pending   int
}

func (m *mockBatch) Abort() error          { return nil }
func (m *mockBatch) Append(v ...any) error { return nil }
func (m *mockBatch) AppendStruct(v any) error {
	if m.appendErr != nil {
		return m.appendErr
	}
	m.appended = append(m.appended, v)
	m.pending++
	return nil
}
func (m *mockBatch) Column(int) driver.BatchColumn { return nil }
func (m *mockBatch) Flush() error                  { return nil }
func (m *mockBatch) IsSent() bool                  { return false }
func (m *mockBatch) Rows() int                     { return len(m.appended) }
func (m *mockBatch) Columns() []column.Interface   { return nil }
func (m *mockBatch) Close() error                  { return nil }
func (m *mockBatch) Send() error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sentSizes = append(m.sentSizes, m.pending)
	m.pending = 0
	return nil
}

// mockConn implements driver.Conn for testing.
type mockConn struct {
	prepareErr     error
	batch          *mockBatch
	prepareCalls   int
	prepareBlock   chan struct{}   // if non-nil, PrepareBatch blocks until closed
	lastPrepareCtx context.Context // captures the context from the last PrepareBatch call
}

func (m *mockConn) PrepareBatch(ctx context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	m.lastPrepareCtx = ctx
	if m.prepareBlock != nil {
		<-m.prepareBlock
	}
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
func (m *mockConn) Ping(_ context.Context) error                                     { return nil }
func (m *mockConn) Stats() driver.Stats                                              { return driver.Stats{} }
func (m *mockConn) Close() error                                                     { return nil }

// testRow is a minimal struct used as the generic type T in tests.
type testRow struct {
	Value int
}

func newTestInserter(t *testing.T, conn *mockConn, cfg *BatchInserterConfig[testRow]) *BatchInserter[testRow] {
	t.Helper()
	return newTestInserterWithTable(t, conn, "test_table", cfg)
}

func newTestInserterWithTable(t *testing.T, conn *mockConn, table string, cfg *BatchInserterConfig[testRow]) *BatchInserter[testRow] {
	t.Helper()
	b, err := NewBatchInserter[testRow](conn, table, cfg)
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
		{
			name: "negative channel buffer",
			cfgFn: func() *BatchInserterConfig[testRow] {
				cfg := DefaultBatchInserterConfig[testRow]()
				cfg.ChannelBuffer = -1
				return cfg
			},
			wantErr: true,
		},
		{
			name: "channel buffer exceeds max batch size",
			cfgFn: func() *BatchInserterConfig[testRow] {
				cfg := DefaultBatchInserterConfig[testRow]()
				cfg.ChannelBuffer = cfg.MaxBatchSize + 1
				return cfg
			},
			wantErr: false,
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
		require.NoError(t, b.Submit(context.Background(), testRow{Value: i}))
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
		require.NoError(t, b.Submit(context.Background(), testRow{Value: i}))
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
		require.NoError(t, b.Submit(context.Background(), testRow{Value: i}))
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

	err := b.Submit(context.Background(), testRow{Value: 1})
	assert.True(t, errors.Is(err, ErrStopped))
}

func TestBatchInserter_Flush_prepareError(t *testing.T) {
	prepareErr := errors.New("prepare failed")
	conn := &mockConn{prepareErr: prepareErr, batch: &mockBatch{}}
	cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 10, FlushInterval: time.Hour}
	b := newTestInserter(t, conn, cfg)
	b.Start(context.Background())

	require.NoError(t, b.Submit(context.Background(), testRow{Value: 1}))

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

	require.NoError(t, b.Submit(context.Background(), testRow{Value: 1}))

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

	require.NoError(t, b.Submit(context.Background(), testRow{Value: 42}))
	_ = b.Flush(context.Background())

	assert.ErrorIs(t, droppedErr, sendErr)
	require.Len(t, droppedRows, 1)
	assert.Equal(t, 42, droppedRows[0].Value)

	require.NoError(t, b.Stop(context.Background()))
}

func TestBatchInserter_Start_flushesOnInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		batch := &mockBatch{}
		conn := &mockConn{batch: batch}
		cfg := &BatchInserterConfig[testRow]{
			MaxBatchSize:  1000,
			FlushInterval: 5 * time.Second,
		}
		b := newTestInserter(t, conn, cfg)
		b.Start(t.Context())

		require.NoError(t, b.Submit(t.Context(), testRow{Value: 42}))

		// Advance the fake clock past FlushInterval. Once all goroutines in
		// the bubble are durably blocked, the fake clock advances automatically.
		time.Sleep(5 * time.Second)
		// Wait for the run goroutine to finish doFlush and return to select.
		synctest.Wait()

		assert.Len(t, batch.appended, 1)
		v, ok := batch.appended[0].(*testRow)
		require.True(t, ok)
		assert.Equal(t, 42, v.Value)

		require.NoError(t, b.Stop(t.Context()))
	})
}

// TestBatchInserter_rowType verifies that AppendStruct receives a pointer to T.
func TestBatchInserter_rowType(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}
	cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 10, FlushInterval: time.Hour}
	b := newTestInserter(t, conn, cfg)
	b.Start(context.Background())

	require.NoError(t, b.Submit(context.Background(), testRow{Value: 99}))
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

	require.NoError(t, b1.Submit(context.Background(), testRow{Value: 1}))
	require.NoError(t, b2.Submit(context.Background(), testRow{Value: 2}))

	require.NoError(t, group.Stop(context.Background()))

	assert.Len(t, batch1.appended, 1)
	assert.Len(t, batch2.appended, 1)
}

func TestBatchInserterGroup_Flush(t *testing.T) {
	batch1 := &mockBatch{}
	conn1 := &mockConn{batch: batch1}
	batch2 := &mockBatch{}
	conn2 := &mockConn{batch: batch2}
	cfg := DefaultBatchInserterConfig[testRow]()

	b1 := newTestInserter(t, conn1, cfg)
	b2 := newTestInserter(t, conn2, cfg)

	group := &BatchInserterGroup{}
	group.Add(b1)
	group.Add(b2)
	group.Start(context.Background())

	require.NoError(t, b1.Submit(context.Background(), testRow{Value: 1}))
	require.NoError(t, b2.Submit(context.Background(), testRow{Value: 2}))

	require.NoError(t, group.Flush(context.Background()))

	assert.Len(t, batch1.appended, 1)
	assert.Len(t, batch2.appended, 1)

	// Submit more rows after flush and flush again to verify re-use.
	require.NoError(t, b1.Submit(context.Background(), testRow{Value: 3}))
	require.NoError(t, group.Flush(context.Background()))

	assert.Len(t, batch1.appended, 2)
	assert.Len(t, batch2.appended, 1)

	require.NoError(t, group.Stop(context.Background()))
}

func TestBatchInserterGroup_Flush_partialError(t *testing.T) {
	batch1 := &mockBatch{}
	conn1 := &mockConn{batch: batch1}
	batch2 := &mockBatch{}
	conn2 := &mockConn{batch: batch2, prepareErr: errors.New("prepare failed")}
	cfg := DefaultBatchInserterConfig[testRow]()

	b1 := newTestInserterWithTable(t, conn1, "table_ok", cfg)
	b2 := newTestInserterWithTable(t, conn2, "table_fail", cfg)

	group := &BatchInserterGroup{}
	group.Add(b1)
	group.Add(b2)
	group.Start(context.Background())

	require.NoError(t, b1.Submit(context.Background(), testRow{Value: 1}))
	require.NoError(t, b2.Submit(context.Background(), testRow{Value: 2}))

	err := group.Flush(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table_fail")

	// The healthy inserter should have flushed successfully.
	assert.Len(t, batch1.appended, 1)

	require.NoError(t, group.Stop(context.Background()))
}

func TestBatchInserter_flushCapsAtMaxBatchSize(t *testing.T) {
	const maxBatch = 3
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}
	cfg := &BatchInserterConfig[testRow]{
		MaxBatchSize:  maxBatch,
		FlushInterval: time.Hour, // won't fire during this test
		ChannelBuffer: maxBatch,
	}
	b := newTestInserter(t, conn, cfg)
	b.Start(t.Context())

	// Submit more rows than MaxBatchSize. The first maxBatch fill the channel
	// buffer; subsequent Submits block until the run goroutine drains rows.
	for i := range 7 {
		require.NoError(t, b.Submit(t.Context(), testRow{Value: i}))
	}

	require.NoError(t, b.Stop(t.Context()))

	// All rows must have been flushed.
	assert.Len(t, batch.appended, 7)

	// No individual send may exceed MaxBatchSize.
	for i, size := range batch.sentSizes {
		assert.LessOrEqual(t, size, maxBatch, "send #%d had %d rows", i, size)
	}
}

func TestNewBatchInserter_invalidTableName(t *testing.T) {
	conn := &mockConn{batch: &mockBatch{}}
	cfg := DefaultBatchInserterConfig[testRow]()

	_, err := NewBatchInserter[testRow](conn, "Robert'; DROP TABLE students;--", cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid characters")
}

func TestNewBatchInserter_channelBufferDefaults(t *testing.T) {
	conn := &mockConn{batch: &mockBatch{}}
	cfg := &BatchInserterConfig[testRow]{
		MaxBatchSize:  50,
		FlushInterval: time.Second,
		ChannelBuffer: 0, // should default to MaxBatchSize
	}
	b, err := NewBatchInserter[testRow](conn, "test_table", cfg)
	require.NoError(t, err)
	assert.Equal(t, 50, cap(b.rowCh))
}

func TestBatchInserter_Stop_reportsRemainingRowsOnFlushError(t *testing.T) {
	sendErr := errors.New("send failed")
	batch := &mockBatch{sendErr: sendErr}
	conn := &mockConn{batch: batch}

	var allDropped []testRow
	cfg := &BatchInserterConfig[testRow]{
		MaxBatchSize:  3,
		FlushInterval: time.Hour,
		ChannelBuffer: 3,
		OnDroppedRows: func(rows []testRow, err error) {
			allDropped = append(allDropped, rows...)
		},
	}
	b := newTestInserter(t, conn, cfg)
	b.Start(t.Context())

	for i := range 5 {
		require.NoError(t, b.Submit(t.Context(), testRow{Value: i}))
	}

	require.NoError(t, b.Stop(t.Context()))

	// All 5 rows must be accounted for via OnDroppedRows, regardless of
	// how they were distributed between buf and rowCh at stop time.
	assert.Len(t, allDropped, 5)
}

func TestBatchInserter_Submit_blockedThenStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		prepareBlock := make(chan struct{})
		batch := &mockBatch{}
		conn := &mockConn{batch: batch, prepareBlock: prepareBlock}
		cfg := &BatchInserterConfig[testRow]{
			MaxBatchSize:  1,
			FlushInterval: time.Hour,
			ChannelBuffer: 1,
		}
		b := newTestInserter(t, conn, cfg)
		b.Start(t.Context())

		// Row 0 enters the channel. The run goroutine reads it and enters
		// sendBatch, which blocks on prepareBlock.
		require.NoError(t, b.Submit(t.Context(), testRow{Value: 0}))
		synctest.Wait()

		// Row 1 fills the now-empty channel (run goroutine is stuck in sendBatch).
		require.NoError(t, b.Submit(t.Context(), testRow{Value: 1}))

		// Row 2 blocks: channel full, run goroutine busy.
		submitErr := make(chan error, 1)
		go func() {
			submitErr <- b.Submit(t.Context(), testRow{Value: 2})
		}()
		synctest.Wait()

		// Unblock sendBatch after a delay so Stop can complete.
		go func() {
			time.Sleep(time.Second)
			close(prepareBlock)
		}()

		require.NoError(t, b.Stop(t.Context()))

		err := <-submitErr
		assert.ErrorIs(t, err, ErrStopped)
		assert.Len(t, conn.batch.appended, 2)
	})
}

func TestBatchInserter_ctxCancelStopsRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn := &mockConn{batch: &mockBatch{}}
		cfg := DefaultBatchInserterConfig[testRow]()
		b := newTestInserter(t, conn, cfg)

		ctx, cancel := context.WithCancel(t.Context())
		b.Start(ctx)

		cancel()
		synctest.Wait()

		// done should be closed after ctx cancellation.
		select {
		case <-b.done:
		default:
			t.Fatal("run goroutine did not exit after context cancellation")
		}
	})
}

type ctxKey string

func TestBatchInserter_Stop_drainUsesStopContext(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}
	cfg := &BatchInserterConfig[testRow]{MaxBatchSize: 10, FlushInterval: time.Hour}
	b := newTestInserter(t, conn, cfg)
	b.Start(context.Background())

	require.NoError(t, b.Submit(context.Background(), testRow{Value: 1}))

	stopCtx := context.WithValue(context.Background(), ctxKey("origin"), "stop")
	require.NoError(t, b.Stop(stopCtx))

	// The drain flush must use the Stop context for sendBatch.
	require.NotNil(t, conn.lastPrepareCtx)
	assert.Equal(t, "stop", conn.lastPrepareCtx.Value(ctxKey("origin")))
}

func TestBatchInserter_flushesOnIntervalThenStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		batch := &mockBatch{}
		conn := &mockConn{batch: batch}
		cfg := &BatchInserterConfig[testRow]{
			MaxBatchSize:  100,
			FlushInterval: 5 * time.Second,
		}
		b := newTestInserter(t, conn, cfg)
		b.Start(t.Context())

		for i := range 3 {
			require.NoError(t, b.Submit(t.Context(), testRow{Value: i}))
		}

		// Advance past flush interval and wait for the interval flush to complete.
		time.Sleep(5 * time.Second)
		synctest.Wait()

		require.NoError(t, b.Stop(t.Context()))

		// All rows must be flushed exactly once.
		assert.Len(t, batch.appended, 3)
	})
}

func TestBatchInserter_Flush_afterStop(t *testing.T) {
	conn := &mockConn{batch: &mockBatch{}}
	b := newTestInserter(t, conn, DefaultBatchInserterConfig[testRow]())
	b.Start(context.Background())
	require.NoError(t, b.Stop(context.Background()))

	err := b.Flush(context.Background())
	assert.ErrorIs(t, err, ErrStopped)
}

func TestBatchInserter_channelBufferLargerThanBatchSize(t *testing.T) {
	batch := &mockBatch{}
	conn := &mockConn{batch: batch}
	cfg := &BatchInserterConfig[testRow]{
		MaxBatchSize:  3,
		FlushInterval: time.Hour,
		ChannelBuffer: 10, // larger than MaxBatchSize
	}
	b := newTestInserter(t, conn, cfg)
	b.Start(t.Context())

	for i := range 7 {
		require.NoError(t, b.Submit(t.Context(), testRow{Value: i}))
	}

	require.NoError(t, b.Stop(t.Context()))

	assert.Len(t, batch.appended, 7)
	assert.Len(t, batch.sentSizes, 3)
	for i, size := range batch.sentSizes {
		assert.LessOrEqual(t, size, 3, "send #%d had %d rows", i, size)
	}
}

// TestBatchInserter_Stop_noSilentDataLoss races many Submit goroutines against
// Stop and verifies that every row is accounted for: either flushed, reported
// via OnDroppedRows, or rejected with ErrStopped. If the CRITICAL review
// finding (rows silently dropped without OnDroppedRows) is real, this test
// should fail at least occasionally under repeated runs or -race.
func TestBatchInserter_Stop_noSilentDataLoss(t *testing.T) {
	const iterations = 1000
	const numSubmitters = 10
	const rowsPerSubmitter = 10
	const totalRows = numSubmitters * rowsPerSubmitter

	for iter := range iterations {
		batch := &mockBatch{}
		conn := &mockConn{batch: batch}

		var mu sync.Mutex
		var droppedCount int
		cfg := &BatchInserterConfig[testRow]{
			MaxBatchSize:  5,
			FlushInterval: time.Hour,
			ChannelBuffer: 10,
			OnDroppedRows: func(rows []testRow, _ error) {
				mu.Lock()
				droppedCount += len(rows)
				mu.Unlock()
			},
		}
		b, err := NewBatchInserter[testRow](conn, "test_table", cfg)
		require.NoError(t, err)
		b.Start(context.Background())

		var rejectedCount atomic.Int64
		var wg sync.WaitGroup
		wg.Add(numSubmitters)

		for i := range numSubmitters {
			go func() {
				defer wg.Done()
				for j := range rowsPerSubmitter {
					if err := b.Submit(context.Background(), testRow{Value: i*rowsPerSubmitter + j}); err != nil {
						rejectedCount.Add(1)
					}
				}
			}()
		}

		// Let submitters race, then stop.
		runtime.Gosched()
		require.NoError(t, b.Stop(context.Background()))
		wg.Wait()

		// After Stop returns (done is closed) and all submitters finished,
		// every row must be accounted for.
		mu.Lock()
		dropped := droppedCount
		mu.Unlock()

		flushed := len(batch.appended)
		rejected := int(rejectedCount.Load())
		accounted := flushed + dropped + rejected

		if accounted != totalRows {
			t.Fatalf("iteration %d: rows not accounted for: flushed=%d dropped=%d rejected=%d total=%d want=%d",
				iter, flushed, dropped, rejected, accounted, totalRows)
		}
	}
}
