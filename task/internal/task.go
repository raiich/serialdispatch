// Package internal holds the [task.Task] and [task.Timer] implementations the
// dispatchers share.
package internal

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/raiich/serialdispatch/task"
)

// Both are held as the interface: converting a CompletedTask value on each
// return would allocate, since the struct is not pointer-shaped.
var (
	// SucceededTask is a [task.Task] whose function already completed without error.
	SucceededTask task.Task = CompletedTask{}
	// CanceledTask is a [task.Task] for a function that a stopped dispatcher will never run.
	CanceledTask task.Task = CompletedTask{Err: task.ErrCanceled}
)

// CompletedTask is a [task.Task] with a fixed result, used when the outcome is
// already known at submission time (e.g. the dispatcher has stopped).
type CompletedTask struct {
	Err error
}

// Wait returns the fixed result and never blocks.
func (t CompletedTask) Wait(context.Context) error {
	return t.Err
}

// PanickedTask is a [task.Task] whose function panicked synchronously during
// submission.
type PanickedTask struct {
	Recovered any
}

// Wait re-panics with the recovered value on every call; it never returns.
func (t PanickedTask) Wait(context.Context) error {
	panic(t.Recovered)
}

// PendingTask is a [task.Task] whose function is queued for later serialized
// execution. Exactly one of Run or Cancel settles it and closes done; Wait
// observes the result afterwards.
type PendingTask struct {
	fn       func()
	done     chan struct{}
	err      error
	panicked any
}

// Wait blocks until the function settles or ctx is done. If ctx is done first,
// it returns context.Cause(ctx) without affecting the function. Once settled, it
// returns the cancellation error, nil on success, or re-panics with the same
// value if the function panicked.
func (t *PendingTask) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		// Prefer an already-settled result so a simultaneously-done ctx never masks
		// it: a settled task means ctx was not done first.
		select {
		case <-t.done:
			return t.result()
		default:
		}
		return context.Cause(ctx)
	case <-t.done:
		return t.result()
	}
}

// result reports the settled outcome. It must be called only after t.done is
// closed: nil on success, the cancellation error, or a re-panic.
func (t *PendingTask) result() error {
	if t.panicked != nil {
		panic(t.panicked)
	}
	return t.err
}

// Run executes the function and settles the task. A panic is recovered and
// returned as an error (also stored so Wait re-panics).
func (t *PendingTask) Run() (err error) {
	defer close(t.done)
	defer func() {
		if r := recover(); r != nil {
			t.panicked = r
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	t.fn()
	return nil
}

// Cancel settles the task as canceled without running the function; the result
// matches [task.ErrCanceled].
func (t *PendingTask) Cancel() {
	t.err = task.ErrCanceled
	close(t.done)
}

// Task returns the handle to hand out for t. It reaches Wait only, so Run and
// Cancel stay with the dispatcher.
func (t *PendingTask) Task() task.Task {
	return waitOnly{t}
}

// waitOnly narrows a PendingTask to [task.Task].
type waitOnly struct {
	t *PendingTask
}

func (w waitOnly) Wait(ctx context.Context) error {
	return w.t.Wait(ctx)
}

// NewPendingTask returns a PendingTask that will run fn when dispatched.
func NewPendingTask(fn func()) *PendingTask {
	return &PendingTask{
		fn:   fn,
		done: make(chan struct{}),
	}
}
