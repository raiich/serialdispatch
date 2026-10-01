// Package mutex provides a task dispatcher that serializes tasks with a
// sync.Mutex instead of a run loop, so it has no goroutine to serve.
package mutex

import (
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/raiich/serialdispatch/task"
	"github.com/raiich/serialdispatch/task/internal"
)

var _ task.Dispatcher = (*Dispatcher)(nil)

// Dispatcher serializes tasks with a sync.Mutex: InvokeFunc runs the function in
// the caller's goroutine, and AfterFunc runs it in the timer's own goroutine.
//
// The zero value is not usable; create one with [NewDispatcher].
type Dispatcher struct {
	// errCh receives the panic error when a function panics. Buffered with size 1:
	// the ended gate ensures at most one panic is ever sent, so the send (done
	// while holding mu) never blocks.
	errCh chan error
	mu    sync.Mutex
	// ended indicates whether the dispatcher has been terminated due to a panic.
	// When true, safeExec runs no further function; submissions settle as canceled.
	ended bool
}

// Err returns a channel that receives an error when the dispatcher stops due to an unrecoverable error.
func (d *Dispatcher) Err() <-chan error {
	return d.errCh
}

// AfterFunc schedules f to execute after the specified duration, serialized by
// the mutex against the other functions of this dispatcher.
//
// See [task.Timer.Stop] for Stop semantics.
func (d *Dispatcher) AfterFunc(duration time.Duration, f func()) task.Timer {
	t := &internal.DispatcherTimer{}
	t.Inner = time.AfterFunc(duration, func() {
		d.mu.Lock()
		defer d.mu.Unlock()

		// Skip when ended: returning before TryFire leaves doNotFire false, so a
		// later Stop reports true (it prevented execution), per the Timer.Stop
		// contract for a function that never ran.
		if d.ended {
			return
		}
		t.TryFire(func() { d.safeExec(f) })
	})
	return t
}

// InvokeFunc runs f synchronously in the caller's goroutine, serialized by the
// mutex against AfterFunc callbacks. It returns an already-settled [task.Task].
func (d *Dispatcher) InvokeFunc(f func()) task.Task {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.ended {
		return internal.CanceledTask
	}
	return d.safeExec(f)
}

// safeExec runs f under panic recovery and returns a settled [task.Task]:
// [internal.SucceededTask] when f completes, or on panic it marks the dispatcher
// ended, reports the panic on errCh, and returns a [internal.PanickedTask]. The
// caller must hold d.mu and must not call it once the dispatcher has ended.
func (d *Dispatcher) safeExec(f func()) (result task.Task) {
	defer func() {
		if r := recover(); r != nil {
			result = internal.PanickedTask{Recovered: r}
			d.ended = true
			d.errCh <- fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	f()
	return internal.SucceededTask
}

// NewDispatcher creates a new Dispatcher.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		errCh: make(chan error, 1),
	}
}
