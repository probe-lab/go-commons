package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ErrStopped is returned by Submit and Flush when called after Stop.
var ErrStopped = errors.New("batch inserter stopped")

var validTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

var (
	attrKeyTable   = attribute.Key("table")
	attrKeyTrigger = attribute.Key("trigger")
)

// BatchInserterConfig holds configuration for a [BatchInserter].
type BatchInserterConfig[T any] struct {
	// MaxBatchSize is the maximum number of rows to buffer before flushing.
	MaxBatchSize int
	// FlushInterval is the maximum time between flushes.
	FlushInterval time.Duration
	// ChannelBuffer is the capacity of the internal row channel. A larger buffer
	// allows callers to enqueue rows without blocking while the run goroutine is
	// busy flushing. The memory cost is ChannelBuffer * sizeof(T), allocated
	// upfront. Defaults to MaxBatchSize when zero.
	ChannelBuffer int
	// Meter is the OTel meter used to record batch inserter metrics. If nil,
	// the global meter provider is used, which is a no-op when
	// [tele.ServeMetrics] has not been called with metrics enabled.
	Meter metric.Meter
	// OnDroppedRows is called when a flush fails and rows are dropped.
	// The slice contains the rows that were lost; the error is the flush error.
	// The callback is always invoked in addition to slog error logging.
	// If nil, only the slog error is emitted.
	OnDroppedRows func(rows []T, err error)
}

// DefaultBatchInserterConfig returns a [BatchInserterConfig] with sensible defaults.
func DefaultBatchInserterConfig[T any]() *BatchInserterConfig[T] {
	return &BatchInserterConfig[T]{
		MaxBatchSize:  1000,
		FlushInterval: 5 * time.Second,
		ChannelBuffer: 1000,
	}
}

// Validate checks the [BatchInserterConfig] for validity.
func (cfg *BatchInserterConfig[T]) Validate() error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	if cfg.MaxBatchSize <= 0 {
		return fmt.Errorf("max batch size must be a positive integer")
	}

	if cfg.FlushInterval <= 0 {
		return fmt.Errorf("flush interval must be a positive duration")
	}

	if cfg.ChannelBuffer < 0 {
		return fmt.Errorf("channel buffer must be a non-negative integer")
	}

	return nil
}

// BatchInserter buffers rows of type T and flushes them to a ClickHouse table
// in batches. A single background goroutine owns the buffer exclusively; all
// coordination happens via channels.
//
// Rows are held in a plain []T slice rather than in an open [driver.Batch].
// Calling [driver.Conn.PrepareBatch] sends a block header to the server and
// ties up connection state for the lifetime of the batch object. Keeping that
// open across an entire flush interval (potentially seconds) is wasteful, and
// a connection hiccup at PrepareBatch time would make the inserter unable to
// accept any rows until the next interval. The []T approach decouples buffering
// from connectivity: rows accumulate regardless of ClickHouse availability, and
// the driver interaction is confined to the brief flush window.
//
// Rows are dropped on flush failure. The [driver.Batch] is a stateful protocol
// object that cannot be retried; re-batching would require a new PrepareBatch
// call anyway. Use [BatchInserterConfig.OnDroppedRows] to handle dropped rows.
//
// Call [BatchInserter.Start] before [BatchInserter.Submit] or [BatchInserter.Flush],
// and [BatchInserter.Stop] to drain and shut down.
type BatchInserter[T any] struct {
	conn  driver.Conn             // ClickHouse connection used for batch inserts
	table string                  // target table name
	cfg   *BatchInserterConfig[T] // user-supplied configuration

	rowCh     chan T               // buffered channel through which Submit sends rows to run
	flushCh   chan chan error      // unbuffered; Flush sends a response channel, run replies with the flush result
	stopCh    chan struct{}        // closed by Stop to signal the run goroutine to drain and exit
	stopCtxCh chan context.Context // buffered(1); Stop sends its context before closing stopCh
	done      chan struct{}        // closed by run when it returns; Stop blocks on this
	submits   atomic.Int32

	startOnce sync.Once // ensures Start launches the run goroutine at most once
	stopOnce  sync.Once // ensures Stop closes stopCh at most once

	// buf holds rows pending flush. Owned exclusively by the run goroutine;
	// initialized when Start is called.
	buf []T

	// OTel instruments; always valid (no-op if metrics not configured).
	mRowsFlushed   metric.Int64Counter     // total rows successfully flushed
	mRowsDropped   metric.Int64Counter     // total rows lost due to flush errors
	mFlushDuration metric.Float64Histogram // time per flush operation (seconds)
	mFlushSize     metric.Int64Histogram   // rows per flush attempt
}

// NewBatchInserter creates a new [BatchInserter]. Call [BatchInserter.Start]
// before [BatchInserter.Submit] or [BatchInserter.Flush].
func NewBatchInserter[T any](conn driver.Conn, table string, cfg *BatchInserterConfig[T]) (*BatchInserter[T], error) {
	if conn == nil {
		return nil, fmt.Errorf("conn must not be nil")
	}

	if table == "" {
		return nil, fmt.Errorf("table must not be empty")
	}

	if !validTableName.MatchString(table) {
		return nil, fmt.Errorf("table name %q contains invalid characters", table)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("batch inserter config: %w", err)
	}

	channelBuffer := cfg.ChannelBuffer
	if channelBuffer == 0 {
		channelBuffer = cfg.MaxBatchSize
	}

	meter := cfg.Meter
	if meter == nil {
		meter = otel.GetMeterProvider().Meter("github.com/probe-lab/go-commons/db")
	}

	b := &BatchInserter[T]{
		conn:      conn,
		table:     table,
		cfg:       cfg,
		rowCh:     make(chan T, channelBuffer),
		flushCh:   make(chan chan error),
		stopCh:    make(chan struct{}),
		stopCtxCh: make(chan context.Context, 1),
		done:      make(chan struct{}),
	}

	// The OTel SDK deduplicates instruments with the same name, type, unit, and
	// description on the same meter — creating the same instrument multiple times
	// returns the existing one rather than erroring. Creating a BatchInserter per
	// table is therefore safe even though all share the same instrument names.
	var err error
	if b.mRowsFlushed, err = meter.Int64Counter("batch_inserter.rows_flushed",
		metric.WithDescription("Total number of rows successfully flushed to ClickHouse"),
	); err != nil {
		slog.Warn("Failed to create metric instrument", "name", "batch_inserter.rows_flushed", "err", err)
	}

	if b.mRowsDropped, err = meter.Int64Counter("batch_inserter.rows_dropped",
		metric.WithDescription("Total number of rows dropped due to flush errors"),
	); err != nil {
		slog.Warn("Failed to create metric instrument", "name", "batch_inserter.rows_dropped", "err", err)
	}

	if b.mFlushDuration, err = meter.Float64Histogram("batch_inserter.flush_duration",
		metric.WithDescription("Duration of each flush operation"),
		metric.WithUnit("s"),
	); err != nil {
		slog.Warn("Failed to create metric instrument", "name", "batch_inserter.flush_duration", "err", err)
	}

	if b.mFlushSize, err = meter.Int64Histogram("batch_inserter.flush_size",
		metric.WithDescription("Number of rows in each flush attempt"),
	); err != nil {
		slog.Warn("Failed to create metric instrument", "name", "batch_inserter.flush_size", "err", err)
	}

	return b, nil
}

// Start launches the background goroutine that owns the row buffer.
// Subsequent calls are no-ops. Must be called before [BatchInserter.Submit] or
// [BatchInserter.Flush].
//
// The context controls the lifetime of normal flush operations (size-triggered,
// interval, and manual). Cancelling it terminates the background goroutine
// immediately without a final drain; use [BatchInserter.Stop] for orderly
// shutdown with a dedicated drain deadline.
func (b *BatchInserter[T]) Start(ctx context.Context) {
	b.startOnce.Do(func() {
		go b.run(ctx)
	})
}

func (b *BatchInserter[T]) run(ctx context.Context) {
	defer close(b.done)

	b.buf = make([]T, 0, b.cfg.MaxBatchSize)
	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case row := <-b.rowCh:
			b.buf = append(b.buf, row)
			if len(b.buf) >= b.cfg.MaxBatchSize {
				_ = b.doFlush(ctx, "size") // error logged and forwarded via OnDroppedRows
			}

		case <-ticker.C:
			_ = b.doFlush(ctx, "interval") // error logged and forwarded via OnDroppedRows

		case resp := <-b.flushCh:
			resp <- b.doFlush(ctx, "manual")

		case <-b.stopCh:
			b.drain(<-b.stopCtxCh)
			return

		case <-ctx.Done():
			return
		}
	}
}

// drain flushes all remaining rows during shutdown. It loops until both buf
// and rowCh are empty. On a flush error, any rows still in rowCh are drained
// and reported via OnDroppedRows. Owned exclusively by the run goroutine.
func (b *BatchInserter[T]) drain(ctx context.Context) {
	// Check submits first: when submits.Load() returns 0, all in-flight
	// Submit calls have completed — including their channel send, which is
	// sequenced before the deferred counter decrement. This guarantees any
	// rows they sent are visible in rowCh by the time we check its length.
	// Checking rowCh or buf first would allow a TOCTOU race where a Submit
	// sends a row and decrements the counter between the rowCh and submits
	// checks, causing drain to exit with an unread row in the channel.
	for b.submits.Load() > 0 || len(b.buf) > 0 || len(b.rowCh) > 0 {
		_ = b.doFlush(ctx, "stop")
		runtime.Gosched() // minimizes busy-looping - not necessary for correctness
	}
}

// doFlush drains rowCh into buf up to MaxBatchSize, then sends the batch.
// Owned exclusively by the run goroutine.
func (b *BatchInserter[T]) doFlush(ctx context.Context, trigger string) error {
	// Drain any rows buffered in rowCh so that a flush issued right
	// after Submit (which returns as soon as the channel accepts the row)
	// captures all pending rows regardless of select ordering.
	// Cap at MaxBatchSize to keep flush payloads bounded.
drain:
	for len(b.buf) < b.cfg.MaxBatchSize {
		select {
		case row := <-b.rowCh:
			b.buf = append(b.buf, row)
		default:
			break drain
		}
	}

	if len(b.buf) == 0 {
		return nil
	}

	// Capture rows and allocate a new backing array for buf so that the
	// rows slice remains stable for the OnDroppedRows callback.
	rows := b.buf
	b.buf = make([]T, 0, b.cfg.MaxBatchSize)

	start := time.Now()
	err := b.sendBatch(ctx, rows)
	elapsed := time.Since(start)

	flushAttrs := metric.WithAttributes(
		attrKeyTable.String(b.table),
		attrKeyTrigger.String(trigger),
	)
	b.mFlushDuration.Record(ctx, elapsed.Seconds(), flushAttrs)
	b.mFlushSize.Record(ctx, int64(len(rows)), flushAttrs)

	if err != nil {
		b.mRowsDropped.Add(ctx, int64(len(rows)), metric.WithAttributes(attrKeyTable.String(b.table)))
		slog.Error("Failed to flush batch",
			"table", b.table,
			"dropped_rows", len(rows),
			"trigger", trigger,
			"err", err,
		)
		if b.cfg.OnDroppedRows != nil {
			b.cfg.OnDroppedRows(rows, err)
		}
		return err
	}

	b.mRowsFlushed.Add(ctx, int64(len(rows)), metric.WithAttributes(attrKeyTable.String(b.table)))
	return nil
}

// Submit submits a row to the batch. It blocks until the background goroutine
// accepts the row, the context is canceled, or Stop has been called.
func (b *BatchInserter[T]) Submit(ctx context.Context, row T) error {
	b.submits.Add(1)
	defer b.submits.Add(-1)

	// Fast-reject: return ErrStopped immediately if Stop has been called.
	// Without this, the second select would pick randomly between rowCh and
	// stopCh when both are ready, allowing ~50% of post-Stop calls to send
	// rows. That is still correct (drain's submits counter ensures those
	// rows are flushed), but it extends shutdown: a caller in a tight loop
	// keeps feeding rows that drain must process. This early check ensures
	// only the few Submits already past this point when stopCh closes can
	// race into the second select, letting drain converge quickly.
	select {
	case <-b.stopCh:
		return fmt.Errorf("add to %s: %w", b.table, ErrStopped)
	case <-b.done:
		return fmt.Errorf("add to %s: %w", b.table, ErrStopped)
	default:
	}

	select {
	case b.rowCh <- row:
		return nil
	case <-b.stopCh:
		return fmt.Errorf("add to %s: %w", b.table, ErrStopped)
	case <-b.done:
		return fmt.Errorf("add to %s: %w", b.table, ErrStopped)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush requests an immediate flush of all buffered rows and blocks until the
// flush completes, the context is canceled, or Stop has been called.
func (b *BatchInserter[T]) Flush(ctx context.Context) error {
	resp := make(chan error, 1)
	select {
	case b.flushCh <- resp:
	case <-b.stopCh:
		return fmt.Errorf("flush %s: %w", b.table, ErrStopped)
	case <-b.done:
		return fmt.Errorf("flush %s: %w", b.table, ErrStopped)
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-resp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop signals the background goroutine to exit and waits for it to finish,
// performing a final flush of any buffered rows. Safe to call multiple times;
// only the first caller's context is used for the drain.
//
// The context controls the final drain flush operations. Use a timeout context
// to bound how long the drain may take. If the context is cancelled before the
// drain completes, remaining rows are dropped via [BatchInserterConfig.OnDroppedRows].
func (b *BatchInserter[T]) Stop(ctx context.Context) error {
	b.stopOnce.Do(func() {
		b.stopCtxCh <- ctx
		close(b.stopCh)
	})

	select {
	case <-b.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *BatchInserter[T]) sendBatch(ctx context.Context, rows []T) error {
	batch, err := b.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", b.table))
	if err != nil {
		return fmt.Errorf("prepare batch for %s: %w", b.table, err)
	}

	for i := range rows {
		if err := batch.AppendStruct(&rows[i]); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append struct to %s: %w", b.table, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch for %s: %w", b.table, err)
	}

	return nil
}

// batchLifecycle is satisfied by [BatchInserter][T], enabling [BatchInserterGroup]
// to manage multiple typed inserters without type parameters.
type batchLifecycle interface {
	Start(ctx context.Context)
	Flush(ctx context.Context) error
	Stop(ctx context.Context) error
}

// BatchInserterGroup manages the lifecycle of multiple [BatchInserter] instances,
// allowing them to be started and stopped with a single call.
type BatchInserterGroup struct {
	inserters []batchLifecycle
}

// Add registers an inserter with the group. Must be called before [BatchInserterGroup.Start].
func (g *BatchInserterGroup) Add(i ...batchLifecycle) {
	g.inserters = append(g.inserters, i...)
}

// Start calls [BatchInserter.Start] on all registered inserters.
func (g *BatchInserterGroup) Start(ctx context.Context) {
	for _, i := range g.inserters {
		i.Start(ctx)
	}
}

// Flush calls [BatchInserter.Flush] on all registered inserters concurrently
// and returns a combined error if any flush failed.
func (g *BatchInserterGroup) Flush(ctx context.Context) error {
	errs := make([]error, len(g.inserters))
	var wg sync.WaitGroup
	for idx, i := range g.inserters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[idx] = i.Flush(ctx)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Stop calls [BatchInserter.Stop] on all registered inserters concurrently
// and returns a combined error if any stop failed.
func (g *BatchInserterGroup) Stop(ctx context.Context) error {
	errs := make([]error, len(g.inserters))
	var wg sync.WaitGroup
	for idx, i := range g.inserters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[idx] = i.Stop(ctx)
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}
