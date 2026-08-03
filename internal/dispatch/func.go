package dispatch

import (
	"context"

	"storm-warning-ledger/internal/domain"
)

// Func adapts a plain function into a Dispatcher.
type Func func(ctx context.Context, ev domain.OutboxEvent) error

func (f Func) Dispatch(ctx context.Context, ev domain.OutboxEvent) error {
	return f(ctx, ev)
}
