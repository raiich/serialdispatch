package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/raiich/serialdispatch/task"
	"github.com/raiich/serialdispatch/task/tasktest"
	"github.com/stretchr/testify/assert"
)

func TestDispatcher(t *testing.T) {
	tasktest.TestDispatcher(t, func(tb testing.TB) (task.Dispatcher, *tasktest.TestHelper) {
		dispatcher := serve(tb)
		return dispatcher.Dispatcher, &tasktest.TestHelper{
			Start: time.Now(),
			AdvanceTo: func(to time.Time) error {
				if dur := time.Until(to); dur > 0 {
					time.Sleep(dur)
				}
				return dispatcher.result()
			},
		}
	})
}

// served is a Dispatcher whose Serve runs on its own goroutine inside the
// synctest bubble.
type served struct {
	*Dispatcher
	// cancel stops Serve; a nil cause makes it return context.Canceled.
	cancel context.CancelCauseFunc
	// done is closed once Serve returned err.
	done chan struct{}
	err  error
}

// serve creates a Dispatcher and starts Serve on a goroutine with a context
// derived from tb.Context(). The context is canceled in tb.Cleanup, which then
// waits for Serve to return.
func serve(tb testing.TB) *served {
	tb.Helper()
	ctx, cancel := context.WithCancelCause(tb.Context())
	s := &served{Dispatcher: NewDispatcher(), cancel: cancel, done: make(chan struct{})}
	go func() {
		s.err = s.Serve(ctx)
		close(s.done)
	}()
	tb.Cleanup(func() {
		cancel(nil)
		<-s.done
	})
	return s
}

// result waits for the bubble to settle and reports what Serve returned, nil
// while it still runs.
func (s *served) result() error {
	synctest.Wait()
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}

// serveUntilStopped starts Serve and stops it by cancelling its context. It
// returns once the dispatcher has fully stopped, so later submissions settle as
// ErrCanceled.
func serveUntilStopped(t *testing.T) *served {
	t.Helper()
	s := serve(t)
	s.cancel(nil)
	synctest.Wait()
	return s
}

func TestDispatcher_InvokeFunc(t *testing.T) {
	t.Run("Task.Wait returns the context cause when ctx ends before the worker runs f, without canceling f", tasktest.WithSyncTest(func(t *testing.T) {
		dispatcher := serve(t)

		release := make(chan struct{})
		dispatcher.InvokeFunc(func() { <-release })
		synctest.Wait() // the worker is blocked inside the first function

		waitCtx, waitCancel := context.WithCancelCause(t.Context())
		cause := errors.New("gave up")
		time.AfterFunc(1*time.Millisecond, func() { waitCancel(cause) })
		var ran atomic.Bool
		pending := dispatcher.InvokeFunc(func() { ran.Store(true) })
		err := pending.Wait(waitCtx)
		assert.ErrorIs(t, err, cause, "Wait returns context.Cause, not ctx.Err")
		assert.False(t, ran.Load(), "f has not run while the worker is blocked")

		close(release)
		assert.NoError(t, pending.Wait(t.Context()), "Wait after giving up reports the result once f ran")
		assert.True(t, ran.Load(), "giving up on Wait does not cancel f")
	}))

	t.Run("the Task reaches Wait only", func(t *testing.T) {
		invoked := NewDispatcher().InvokeFunc(func() {})
		assert.NotImplements(t, (*interface{ Run() error })(nil), invoked, "the holder of a Task must not run the function")
		assert.NotImplements(t, (*interface{ Cancel() })(nil), invoked, "the holder of a Task must not settle the function")
	})

	t.Run("after Serve returned hands out a settled Task", tasktest.WithSyncTest(func(t *testing.T) {
		dispatcher := serveUntilStopped(t)
		// Settled before Wait: it reports ErrCanceled even through a ctx that is
		// already done.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		var ran atomic.Bool
		err := dispatcher.InvokeFunc(func() { ran.Store(true) }).Wait(ctx)
		assert.ErrorIs(t, err, task.ErrCanceled)
		assert.False(t, ran.Load(), "function should not run on a stopped dispatcher")
	}))

	t.Run("after Serve returned, concurrent submissions hand out settled Tasks", tasktest.WithSyncTest(func(t *testing.T) {
		// As above, even while the submissions race with each other's drains of
		// the queue: a Task another submission still holds is not settled. The race
		// is rare, so the case repeats it.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		for range 20 {
			dispatcher := serveUntilStopped(t)
			const numGoroutines = 200
			var wg sync.WaitGroup
			var ran atomic.Bool
			errs := make([]error, numGoroutines)
			for i := range errs {
				wg.Go(func() {
					errs[i] = dispatcher.InvokeFunc(func() { ran.Store(true) }).Wait(ctx)
				})
			}
			wg.Wait()

			for _, err := range errs {
				assert.ErrorIs(t, err, task.ErrCanceled)
			}
			assert.False(t, ran.Load(), "function should not run on a stopped dispatcher")
		}
	}))
}

func TestDispatcher_Serve(t *testing.T) {
	t.Run("context cancellation returns the cause", tasktest.WithSyncTest(func(t *testing.T) {
		dispatcher := serve(t)

		cause := errors.New("stop")
		dispatcher.cancel(cause)

		assert.ErrorIs(t, dispatcher.result(), cause, "Serve should return the cancellation cause")
	}))

	t.Run("no task starts after a task cancels the context", tasktest.WithSyncTest(func(t *testing.T) {
		// Serve's select picks at random once the context is done, so a
		// regression shows in about half of the runs: repeat.
		for range 30 {
			dispatcher := serve(t)

			var ran atomic.Bool
			canceler := dispatcher.InvokeFunc(func() { dispatcher.cancel(nil) })
			next := dispatcher.InvokeFunc(func() { ran.Store(true) })
			timer := dispatcher.AfterFunc(0, func() { ran.Store(true) })

			assert.ErrorIs(t, dispatcher.result(), context.Canceled)
			assert.NoError(t, canceler.Wait(t.Context()), "the task that canceled the context ran to completion")
			assert.ErrorIs(t, next.Wait(t.Context()), task.ErrCanceled, "the task queued behind the cancel settles as canceled")
			assert.True(t, timer.Stop(), "the timer queued behind the cancel reports it prevented execution")
			assert.False(t, ran.Load(), "no task starts once the context is done")
		}
	}))

	t.Run("runs the tasks submitted before it started", tasktest.WithSyncTest(func(t *testing.T) {
		dispatcher := NewDispatcher()
		var ran atomic.Bool
		submitted := dispatcher.InvokeFunc(func() { ran.Store(true) })

		ctx, cancel := context.WithCancel(t.Context())
		go func() { _ = dispatcher.Serve(ctx) }()
		t.Cleanup(func() {
			cancel()
			synctest.Wait()
		})

		assert.NoError(t, submitted.Wait(t.Context()))
		assert.True(t, ran.Load())
	}))

	t.Run("with a done context returns its cause and runs no task", tasktest.WithSyncTest(func(t *testing.T) {
		// Serve's select picks at random, so a regression shows in about half of
		// the runs: repeat.
		for range 30 {
			dispatcher := NewDispatcher()
			var ran atomic.Bool
			submitted := dispatcher.InvokeFunc(func() { ran.Store(true) })

			ctx, cancel := context.WithCancelCause(t.Context())
			cause := errors.New("stop")
			cancel(cause)

			assert.ErrorIs(t, dispatcher.Serve(ctx), cause)
			assert.ErrorIs(t, submitted.Wait(t.Context()), task.ErrCanceled)
			assert.False(t, ran.Load())
		}
	}))

	t.Run("concurrent call returns ErrServed", tasktest.WithSyncTest(func(t *testing.T) {
		dispatcher := serve(t)
		synctest.Wait()

		assert.ErrorIs(t, dispatcher.Serve(t.Context()), ErrServed, "second Serve while running should return ErrServed")
	}))

	t.Run("call after stop returns ErrServed", tasktest.WithSyncTest(func(t *testing.T) {
		dispatcher := serveUntilStopped(t)

		assert.ErrorIs(t, dispatcher.result(), context.Canceled)
		assert.ErrorIs(t, dispatcher.Serve(t.Context()), ErrServed, "Serve after stop should return ErrServed")
	}))
}

// overfilled is a served Dispatcher whose worker is blocked inside a function
// while more functions than the queue holds were submitted, each from a
// goroutine of its own that then waits on its Task, so that the submitters
// beyond the capacity are blocked on the send.
type overfilled struct {
	*served
	// errs holds what each submitter's Wait returned.
	errs []error
	// release lets the worker continue.
	release chan struct{}
	// submitted is done once every submitter returned.
	submitted sync.WaitGroup
}

// overfill serves a Dispatcher and overfills it with f.
func overfill(t *testing.T, f func()) *overfilled {
	t.Helper()
	o := &overfilled{served: serve(t), release: make(chan struct{})}
	o.InvokeFunc(func() { <-o.release })
	synctest.Wait() // the worker is inside the function, so nothing drains the queue

	o.errs = make([]error, cap(o.queue)+10)
	for i := range o.errs {
		o.submitted.Go(func() { o.errs[i] = o.InvokeFunc(f).Wait(t.Context()) })
	}
	synctest.Wait() // the submitters beyond the capacity are blocked on the send
	return o
}

func TestDispatcher_QueueBehavior(t *testing.T) {
	t.Run("submissions beyond the capacity run once the worker drains the queue", tasktest.WithSyncTest(func(t *testing.T) {
		var completed int
		o := overfill(t, func() { completed++ })

		close(o.release)
		o.submitted.Wait()
		for _, err := range o.errs {
			assert.NoError(t, err)
		}
		assert.Equal(t, len(o.errs), completed, "every submission runs")
	}))

	t.Run("submitters blocked on the full queue settle as canceled when Serve stops", tasktest.WithSyncTest(func(t *testing.T) {
		o := overfill(t, func() {})

		// The context is done before the worker leaves the function, so Serve stops
		// without running another task and has to release the blocked sends itself.
		o.cancel(nil)
		close(o.release)
		o.submitted.Wait()
		assert.ErrorIs(t, o.result(), context.Canceled)
		for _, err := range o.errs {
			assert.ErrorIs(t, err, task.ErrCanceled)
		}
	}))

	// A send can land in the queue after Serve's own drain, and only the drain in
	// enqueue cancels such a task. The race is rare, so each case repeats it.

	t.Run("an InvokeFunc racing the stop settles", tasktest.WithSyncTest(func(t *testing.T) {
		// A task left undrained blocks its Wait for good, which synctest reports as
		// a deadlock. A task drained twice panics on closing its channel again.
		for range 200 {
			serveRacingSubmissions(t, func(d *Dispatcher) {
				err := d.InvokeFunc(func() {}).Wait(t.Context())
				if err != nil && !errors.Is(err, task.ErrCanceled) {
					t.Errorf("unexpected Wait error: %v", err)
				}
			})
		}
	}))

	t.Run("an AfterFunc racing the stop leaves no task in the queue", tasktest.WithSyncTest(func(t *testing.T) {
		// AfterFunc returns no Task to wait on, so the check is that the queue is
		// empty once the timer goroutines finish.
		for range 200 {
			s := serveRacingSubmissions(t, func(d *Dispatcher) {
				d.AfterFunc(0, func() {})
			})
			synctest.Wait()
			assert.Empty(t, s.queue, "a task landed after the stop and was not drained")
		}
	}))
}

// serveRacingSubmissions serves a Dispatcher and stops it while 50 goroutines
// submit through submit. It returns once Serve and the submitters returned.
func serveRacingSubmissions(t *testing.T, submit func(d *Dispatcher)) *served {
	t.Helper()
	s := serve(t)
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() { submit(s.Dispatcher) })
	}
	s.cancel(nil)
	wg.Wait()
	<-s.done
	return s
}
