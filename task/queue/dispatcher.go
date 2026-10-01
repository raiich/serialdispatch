// Package queue provides a task dispatcher that executes tasks sequentially in a queue.
package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/raiich/serialdispatch/task"
	"github.com/raiich/serialdispatch/task/internal"
)

var _ task.Dispatcher = (*Dispatcher)(nil)

// ErrServed is returned by Serve when it has already been called on the same
// Dispatcher, whether the prior call is still running or has already returned.
var ErrServed = errors.New("queue: Serve already called")

// Dispatcher executes tasks sequentially in the run loop of its Serve method.
// Functions submitted via AfterFunc and InvokeFunc are buffered in a channel and
// run one at a time on the goroutine that calls Serve.
//
// The dispatcher has no Stop method: cancel the context passed to Serve to stop
// it. Serve also self-stops when a task panics, after which submissions settle as
// [task.ErrCanceled] without the context being canceled.
//
// InvokeFunc from a task blocks while the queue is full, and Wait from a task
// blocks until its ctx is done; a task schedules follow-up work with
// AfterFunc(0, f).
//
// The zero value is not usable; create one with [NewDispatcher].
type Dispatcher struct {
	queue  chan *internal.PendingTask
	closed chan struct{}
	served atomic.Bool
}

// Serve runs queued tasks until the context is canceled or a task panics, and
// returns the error that caused it to stop. A task that cancels the context is
// the last to run. After cancellation from another goroutine, at most one more
// task may start, and none if the context was done before the call; once Serve
// returns, no task runs, and tasks still queued as well as later submissions
// settle as [task.ErrCanceled].
//
// Serve runs at most once per Dispatcher. A concurrent or subsequent call
// returns [ErrServed] without affecting the run loop. To serve again, create a
// new Dispatcher.
func (d *Dispatcher) Serve(serveCtx context.Context) error {
	if !d.served.CompareAndSwap(false, true) {
		return ErrServed
	}
	defer func() {
		close(d.closed)
		d.drain()
	}()

	for {
		select {
		case <-serveCtx.Done():
			return context.Cause(serveCtx)
		case nextTask := <-d.queue:
			// select picks at random when both cases are ready, so a task that
			// canceled the context could otherwise be followed by another.
			if serveCtx.Err() != nil {
				nextTask.Cancel()
				return context.Cause(serveCtx)
			}
			if err := nextTask.Run(); err != nil {
				return err
			}
		}
	}
}

// AfterFunc enqueues f to the task queue after the specified duration.
//
// See [task.Timer.Stop] for Stop semantics.
func (d *Dispatcher) AfterFunc(duration time.Duration, f func()) task.Timer {
	t := &internal.DispatcherTimer{}
	t.Inner = time.AfterFunc(duration, func() {
		// Wrap f in TryFire so a Stop that wins the race against execution
		// prevents f even after the task has been enqueued.
		d.enqueue(internal.NewPendingTask(func() {
			t.TryFire(f)
		}))
	})
	return t
}

// InvokeFunc enqueues f for the worker (Serve) and returns a [task.Task] to wait
// on its completion. Once Serve has returned, the Task is already settled as
// [task.ErrCanceled].
func (d *Dispatcher) InvokeFunc(f func()) task.Task {
	item := internal.NewPendingTask(f)
	d.enqueue(item)
	return item.Task()
}

// enqueue sends item to the queue, or settles it as canceled once Serve has
// stopped.
func (d *Dispatcher) enqueue(item *internal.PendingTask) {
	// Once Serve has stopped, cancel here rather than leave the choice to the
	// select below, so that the item is settled when the caller gets its handle.
	select {
	case <-d.closed:
		item.Cancel()
		return
	default:
	}
	// Send without blocking first: the select on both channels costs more than
	// the send itself and is needed only to wait for room.
	select {
	case d.queue <- item:
	default:
		select {
		case <-d.closed:
			item.Cancel()
			return
		case d.queue <- item:
		}
	}
	// The send can land after Serve's shutdown drained the queue. Serve closes
	// closed before that drain, so a check here sees every such task. Each task
	// is received once, so concurrent drains settle each of them exactly once.
	select {
	case <-d.closed:
		d.drain()
	default:
	}
}

// drain cancels every task left in the queue. Serve calls it once stopped, and
// enqueue after a send that raced with the stop.
func (d *Dispatcher) drain() {
	for {
		select {
		case nextTask := <-d.queue:
			nextTask.Cancel()
		default:
			return
		}
	}
}

// NewDispatcher creates a Dispatcher. Run its tasks by calling Serve.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		queue:  make(chan *internal.PendingTask, 128),
		closed: make(chan struct{}),
	}
}
