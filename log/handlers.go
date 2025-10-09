package log

import (
	"context"
	"log/slog"
)

type handler struct {
	slog.Handler
}

var _ slog.Handler = (*handler)(nil)

func (h *handler) Handle(ctx context.Context, record slog.Record) error {
	// do custom logic to attach context-related fields to the record
	return h.Handler.Handle(ctx, record)
}
