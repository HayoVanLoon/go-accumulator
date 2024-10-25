package accumulator_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/HayoVanLoon/go-accumulator"
)

func TestNew(t *testing.T) {
	type args struct {
		input       []int
		noTrigger   bool
		triggerErr  bool
		blockOnProc bool
	}
	type want struct {
		value   int
		timeout bool
		err     require.ErrorAssertionFunc
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			"simple",
			args{input: validInput},
			want{
				value: 15,
				err:   require.NoError,
			},
		},
		{
			"no input",
			args{},
			want{
				value: 0,
				err:   require.NoError,
			},
		},
		{
			"no trigger",
			args{
				input:     validInput,
				noTrigger: true,
			},
			want{
				timeout: true,
				err:     deadlineExceeded,
			},
		},
		{
			"error in trigger",
			args{
				input:      validInput,
				triggerErr: true,
			},
			want{err: isTestError},
		},
		{
			"block during processing",
			args{
				input:       validInput,
				blockOnProc: true,
			},
			want{
				timeout: true,
				err:     deadlineExceeded,
			},
		},
		{
			"error during processing",
			args{input: []int{0, 1, 2, -4, 8}},
			want{err: isTestError},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			trigger, done := triggerFn(tt.args.triggerErr)
			proc, result := procFn(tt.args.blockOnProc)
			acc := accumulator.OnTriggerFunc(ctx, trigger, proc)

			for _, x := range tt.args.input {
				require.True(t, acc.Insert(x))
			}

			if !tt.args.noTrigger {
				close(done)
				time.Sleep(10 * time.Millisecond)
				// should always be rejected
				for _, x := range []int{16, 32} {
					require.False(t, acc.Insert(x))
				}
			}

			select {
			case <-ctx.Done():
				if !tt.want.timeout {
					require.Fail(t, "test timed out")
				}
				deadlineExceeded(t, ctx.Err())
				// allow time for error to propagate through Accumulator as well
				time.Sleep(10 * time.Millisecond)
				break
			case <-acc.Done():
			}

			select {
			case actual := <-result:
				require.Equal(t, tt.want.value, actual)
			default:
				tt.want.err(t, acc.Err())
			}
		})
	}
}

func TestOnValueFunc(t *testing.T) {
	type args struct {
		input       []int
		onValue     int
		errAt       int
		blockOnProc bool
	}
	type want struct {
		value   int
		timeout bool
		err     require.ErrorAssertionFunc
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			"simple",
			args{
				input:   validInput,
				onValue: 8,
			},
			want{
				value: 15,
				err:   require.NoError,
			},
		},
		{
			"no input",
			args{
				onValue: 8,
			},
			want{
				timeout: true,
				err:     deadlineExceeded,
			},
		},
		{
			"trigger error",
			args{
				input:   validInput,
				onValue: 8,
				errAt:   2,
			},
			want{
				err: isTestError,
			},
		},
		{
			"trigger value not present",
			args{
				input:   validInput,
				onValue: 42,
			},
			want{
				timeout: true,
				err:     deadlineExceeded,
			},
		},
		{
			"block during processing",
			args{
				input:       validInput,
				onValue:     8,
				blockOnProc: true,
			},
			want{
				timeout: true,
				err:     deadlineExceeded,
			},
		},
		{
			"error during processing",
			args{
				input:   []int{0, 1, 2, -4, 8},
				onValue: 8,
			},
			want{err: isTestError},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			proc, result := procFn(tt.args.blockOnProc)
			trigger := valueFunc(tt.args.onValue, validInput[tt.args.errAt])
			acc := accumulator.OnValueFunc(ctx, trigger, proc)

			for i, x := range tt.args.input {
				shouldAccept := x <= tt.args.onValue &&
					(tt.args.errAt == 0 || i < tt.args.errAt)
				require.Equal(t, shouldAccept, acc.Insert(x), "%d", i)
			}

			if slices.Contains(tt.args.input, tt.args.onValue) {
				time.Sleep(10 * time.Millisecond)
			}

			select {
			case <-ctx.Done():
				if !tt.want.timeout {
					require.Fail(t, "test timed out")
				}
				deadlineExceeded(t, ctx.Err())
				// allow time for error to propagate through Accumulator as well
				time.Sleep(10 * time.Millisecond)
				break
			case <-acc.Done():
			}

			select {
			case actual := <-result:
				require.Equal(t, tt.want.value, actual)
			default:
				tt.want.err(t, acc.Err())
				to := tt.args.blockOnProc ||
					!slices.Contains(tt.args.input, tt.args.onValue)
				require.Equal(t, tt.want.timeout, to)
			}
		})
	}
}

var validInput = []int{0, 1, 2, 4, 8}

var testError = fmt.Errorf("oh noes")

func triggerFn(err bool) (accumulator.TriggerFunc, chan struct{}) {
	ch := make(chan struct{})
	fn := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			if err {
				return testError
			}
			return nil
		}
	}
	return fn, ch
}

func procFn(block bool) (accumulator.ProcessFunc[int], <-chan int) {
	ch := make(chan int, 1)
	fn := func(ctx context.Context, xs []int) error {
		if block {
			<-ctx.Done()
			return ctx.Err()
		}

		acc := 0
		for _, x := range xs {
			if x < 0 {
				return testError
			}
			acc += x
		}
		ch <- acc
		close(ch)
		return nil
	}
	return fn, ch
}

func valueFunc(target int, errOn int) accumulator.ValueTrigger[int] {
	return func(i int) (bool, error) {
		if errOn != 0 && i == errOn {
			return false, testError
		}
		return i == target, nil
	}
}

func deadlineExceeded(t require.TestingT, err error, _ ...interface{}) {
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func isTestError(t require.TestingT, err error, _ ...interface{}) {
	require.ErrorIs(t, err, testError)
}

func TestCountingTrigger(t *testing.T) {
	const maxTest = 10
	type want struct {
		inserts int
	}
	tests := []struct {
		name string
		args int
		want want
	}{
		{
			"simple",
			3,
			want{3},
		},
		{
			"immediately",
			1,
			want{1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := accumulator.CountingTrigger[struct{}](tt.args)
			for i := range maxTest {
				actual, err := fn(struct{}{})
				require.NoError(t, err)
				require.Equalf(t, tt.want.inserts < i+1, actual, "%d: %v", i, actual)
			}
		})
	}
}
