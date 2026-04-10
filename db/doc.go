// Package db provides database connectivity and batch insertion utilities
// for ClickHouse and PostgreSQL.
//
// # ClickHouse Batch Insertion
//
// [BatchInserter] buffers rows of a given struct type in memory and flushes
// them to ClickHouse in batches, either when the buffer reaches [BatchInserterConfig.MaxBatchSize]
// or when [BatchInserterConfig.FlushInterval] elapses. This matches ClickHouse's
// optimal write pattern of thousands of rows per insert.
//
// Struct fields are mapped to ClickHouse columns via `ch` struct tags, as
// required by the underlying clickhouse-go driver.
//
// Basic usage:
//
//	type VisitRow struct {
//	    Timestamp time.Time `ch:"timestamp"`
//	    PeerID    string    `ch:"peer_id"`
//	}
//
//	conn, err := db.DefaultClickHouseConfig("myapp").OpenAndPing(ctx)
//	if err != nil { ... }
//
//	cfg := db.DefaultBatchInserterConfig[VisitRow]()
//	cfg.MaxBatchSize = 5000
//	cfg.OnDroppedRows = func(rows []VisitRow, err error) {
//	    // Called when a flush fails. Rows are already logged by the inserter.
//	    droppedRowsCounter.Submit(ctx, int64(len(rows)))
//	}
//
//	inserter, err := db.NewBatchInserter[VisitRow](conn, "visits", cfg)
//	if err != nil { ... }
//
//	// Start with a long-lived context; normal flushes use this context.
//	inserter.Start(context.Background())
//
//	// Submit rows from any goroutine; Submit blocks if the inserter is mid-flush.
//	if err := inserter.Submit(ctx, VisitRow{Timestamp: time.Now(), PeerID: "Qm..."}); err != nil { ... }
//
//	// On shutdown, Stop drains buffered rows using its own context as the
//	// flush deadline — independent of the Start context.
//	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//	if err := inserter.Stop(shutdownCtx); err != nil { ... }
//
// # Multiple Tables
//
// Applications typically write to several tables. Create one [BatchInserter]
// per table (each runs a single goroutine — the overhead is negligible) and
// use [BatchInserterGroup] for coordinated lifecycle management:
//
//	visitInserter, _ := db.NewBatchInserter[VisitRow](conn, "visits", visitCfg)
//	dialInserter,  _ := db.NewBatchInserter[DialRow](conn, "dials", dialCfg)
//
//	group := &db.BatchInserterGroup{}
//	group.Add(visitInserter)
//	group.Add(dialInserter)
//	group.Start(context.Background())
//
//	// ... submit rows ...
//
//	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//	if err := group.Stop(shutdownCtx); err != nil { ... }
//
// # Metrics
//
// [BatchInserter] emits OpenTelemetry metrics automatically via the global
// meter provider (configured by [tele.ServeMetrics]). The following instruments
// are recorded with a "table" attribute and, where relevant, a "trigger" attribute
// indicating what caused the flush ("size", "interval", "manual", or "stop"):
//
//   - batch_inserter.rows_flushed   (counter)   — successfully inserted rows
//   - batch_inserter.rows_dropped   (counter)   — rows lost due to flush errors
//   - batch_inserter.flush_duration (histogram) — time taken per flush, in seconds
//   - batch_inserter.flush_size     (histogram) — number of rows per flush attempt
//
// To use a custom meter instead of the global one, set [BatchInserterConfig.Meter].
package db
