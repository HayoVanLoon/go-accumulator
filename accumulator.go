package accumulator

import (
	"context"
	"sync"
	"time"
)

// An Accumulator processes inserted data following a trigger event.
type Accumulator[T any] interface {
	// Insert inserts an item into the accumulator, returning true until the
	// accumulator stops accepting new items.
	//
	// When this method returns (or false), it is guaranteed the item is
	// included in (or excluded from) the final processing, assuming no errors
	// occur.
	Insert(t T) bool
	// Done returns a channel that's closed when the accumulator ceases all
	// operation. This will happen some time after the accumulator stops
	// accepting new values or an error occurs.
	Done() <-chan struct{}
	// Err returns the encountered error, after the accumulator has ceased
	// operation. Should only be called after the Done channel closes. Returns
	// nil while the accumulator is still active.
	Err() error
}

// A TriggerFunc awaits its trigger event, upon which it returns nil. If it
// returns a non-nil error, processing is aborted.
type TriggerFunc func(context.Context) error

// A ValueTrigger returns true if the value matches the trigger condition. If
// it returns a non-nil error, processing is aborted.
type ValueTrigger[T any] func(T) (bool, error)

// A ProcessFunc processes the accumulated data.
type ProcessFunc[T any] func(context.Context, []T) error

type accumulator[T any] struct {
	trigger      chan error
	triggerValue func(T) (bool, error)
	process      ProcessFunc[T]
	close        func() error

	in chan T
	//in  *channel[T]
	buf []T

	mu     sync.RWMutex
	closed bool
	done   chan struct{}
	err    error
}

func (acc *accumulator[T]) run(ctx context.Context) (retErr error) {
	defer func() {
		if err := acc.Close(); retErr == nil {
			retErr = err
		}
		if retErr != nil {
			acc.mu.Lock()
			acc.err = retErr
			acc.mu.Unlock()
		}
	}()

	err := acc.iterate(ctx)
	acc.mu.Lock()
	acc.closed = true
	close(acc.in)
	acc.mu.Unlock()
	if err != nil {
		return err
	}

	for x := range acc.in {
		acc.buf = append(acc.buf, x)
	}
	return acc.process(ctx, acc.buf)
}

func (acc *accumulator[T]) iterate(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-acc.trigger:
			return err
		case x := <-acc.in:
			acc.buf = append(acc.buf, x)
		}
	}
}

func (acc *accumulator[T]) Insert(t T) (accepted bool) {
	acc.mu.Lock()
	defer acc.mu.Unlock()
	if acc.closed {
		return false
	}
	if acc.triggerValue == nil {
		acc.in <- t
		return true
	}

	triggered, err := acc.triggerValue(t)
	if accepted = err == nil; accepted {
		acc.in <- t
	}
	if triggered || err != nil {
		acc.closed = true
		acc.trigger <- err
	}
	return
}

func (acc *accumulator[T]) Done() <-chan struct{} {
	acc.mu.Lock()
	defer acc.mu.Unlock()
	if acc.done == nil {
		acc.done = make(chan struct{})
	}
	return acc.done
}

func (acc *accumulator[T]) Err() error {
	acc.mu.RLock()
	defer acc.mu.RUnlock()
	return acc.err
}

func (acc *accumulator[T]) Close() error {
	acc.mu.Lock()
	defer acc.mu.Unlock()
	if acc.done == nil {
		acc.done = make(chan struct{})
	}
	close(acc.done)
	if acc.close == nil {
		return nil
	}
	return acc.close()
}

// Timeout creates an accumulator that will trigger after the given duration.
func Timeout[T any](ctx context.Context, d time.Duration, fn ProcessFunc[T]) Accumulator[T] {
	trigger := durationTrigger(d)
	return New(ctx, trigger, fn)
}

func durationTrigger(d time.Duration) TriggerFunc {
	return func(ctx context.Context) error {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
		return nil
	}
}

func New[T any](ctx context.Context, trigger TriggerFunc, proc ProcessFunc[T]) Accumulator[T] {
	acc := &accumulator[T]{
		trigger: toTrigger(ctx, trigger),
		process: proc,
		in:      make(chan T, 1),
	}
	go func() { _ = acc.run(ctx) }()
	return acc
}

func OnValue[T comparable](ctx context.Context, t T, proc ProcessFunc[T]) Accumulator[T] {
	trigger := func(in T) (bool, error) { return in == t, nil }
	return OnValueFunc(ctx, trigger, proc)
}

func OnValueFunc[T comparable](ctx context.Context, trigger ValueTrigger[T], proc ProcessFunc[T]) Accumulator[T] {
	acc := &accumulator[T]{
		trigger:      make(chan error, 1),
		triggerValue: trigger,
		process:      proc,
		in:           make(chan T, 1),
	}
	go func() { _ = acc.run(ctx) }()
	return acc
}

func toTrigger(ctx context.Context, fn TriggerFunc) chan error {
	ch := make(chan error, 1)
	go func() {
		ch <- fn(ctx)
		close(ch)
	}()
	return ch
}
