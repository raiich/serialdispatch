// Package tasktest provides the conformance tests every [task.Dispatcher]
// implementation is expected to pass, and the benchmarks they share.
package tasktest

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raiich/serialdispatch/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHelper drives the time of the Dispatcher under test.
type TestHelper struct {
	// Start is the time at which the dispatcher was created.
	Start time.Time
	// AdvanceTo moves time forward to the absolute time to and returns once every
	// function due by then has run, with the error the dispatcher reported for a
	// panicking function. Once the dispatcher stopped, whether a later call reports
	// the panic again is implementation-dependent.
	AdvanceTo func(to time.Time) error
}

// Advance moves time forward to Start + sinceStart and fails the test if the
// dispatcher reported an error.
func (h *TestHelper) Advance(t *testing.T, sinceStart time.Duration) {
	t.Helper()
	require.NoError(t, h.AdvanceTo(h.Start.Add(sinceStart)))
}

// SetupFunc creates a fresh Dispatcher and TestHelper for a single test case.
// It is called inside a synctest bubble and may register cleanups via tb.Cleanup.
type SetupFunc func(tb testing.TB) (task.Dispatcher, *TestHelper)

// run registers body as a subtest named name, running it inside a synctest bubble
// with a fresh dispatcher from setup.
func run(t *testing.T, setup SetupFunc, name string, body func(t *testing.T, d task.Dispatcher, h *TestHelper)) {
	t.Run(name, WithSyncTest(func(t *testing.T) {
		d, h := setup(t)
		body(t, d, h)
	}))
}

// The dispatcher under test may run functions on another goroutine. Their writes
// are visible to the test goroutine once AdvanceTo returns, but a read the test
// goroutine made earlier, asserting a function has not run yet, is not ordered
// before a later write: synctest.Wait orders the writes of goroutines that blocked
// or exited before the waiter, not the waiter's own earlier reads before their
// later writes. The tests therefore read shared state through
// atomics only. Where serialized execution itself is under test, the functions
// mutate a plain variable, so the race detector catches concurrent execution, and
// mirror it into an atomic for the assertion.

// TestDispatcher runs the common Dispatcher conformance tests.
// Each Dispatcher implementation should call this from its own package test,
// passing a SetupFunc.
func TestDispatcher(t *testing.T, setup SetupFunc) {
	t.Helper()
	t.Run("AfterFunc", func(t *testing.T) {
		testAfterFunc(t, setup)
	})
	t.Run("TimerStop", func(t *testing.T) {
		testTimerStop(t, setup)
	})
	t.Run("InvokeFunc", func(t *testing.T) {
		testInvokeFunc(t, setup)
	})
	t.Run("Panic", func(t *testing.T) {
		testPanic(t, setup)
	})
}

func testAfterFunc(t *testing.T, setup SetupFunc) {
	run(t, setup, "executes once at exactly its delay", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		var actual atomic.Int32
		d.AfterFunc(5*time.Millisecond, func() {
			actual.Add(1)
		})
		h.Advance(t, 5*time.Millisecond-1)
		assert.Equal(t, int32(0), actual.Load(), "should not fire before its delay")
		h.Advance(t, 5*time.Millisecond)
		assert.Equal(t, int32(1), actual.Load(), "should fire at exactly its delay")
		h.Advance(t, 55*time.Millisecond)
		assert.Equal(t, int32(1), actual.Load(), "function should execute exactly once")
	})

	run(t, setup, "negative duration", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		var actual, nested atomic.Bool
		timer := d.AfterFunc(-time.Second, func() {
			actual.Store(true)
			// Negative is equivalent to zero: a timer scheduled from the callback is
			// relative to the current time, not to a clock rewound by the delay.
			d.AfterFunc(10*time.Millisecond, func() { nested.Store(true) })
		})
		require.NotNil(t, timer, "Timer should be returned even for negative duration")
		h.Advance(t, 0)
		assert.True(t, actual.Load(), "function should execute for negative duration")
		h.Advance(t, 10*time.Millisecond-1)
		assert.False(t, nested.Load(), "nested timer should not fire before its own delay")
		h.Advance(t, 10*time.Millisecond)
		assert.True(t, nested.Load(), "nested timer should fire at its own delay from the current time")
	})

	run(t, setup, "max duration", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		var actual atomic.Bool
		timer := d.AfterFunc(time.Duration(1<<63-1), func() { actual.Store(true) })
		// Advancing first catches a deadline that overflows into the past, which a
		// Stop right after AfterFunc would still win against.
		h.Advance(t, time.Hour)
		assert.False(t, actual.Load(), "should not fire before its delay")
		assert.True(t, timer.Stop(), "Timer with max duration should be cancellable")
	})

	run(t, setup, "runs in the order the delays elapse within one advance", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		// Each function appends its digit, so the value reads as the order they ran in.
		var v int32
		var actual atomic.Int32
		d.AfterFunc(20*time.Millisecond, func() { v = v*10 + 4; actual.Store(v) })
		d.AfterFunc(12*time.Millisecond, func() { v = v*10 + 2; actual.Store(v) })
		d.AfterFunc(10*time.Millisecond, func() {
			v = v*10 + 1
			actual.Store(v)
			// Due at 15ms: relative to the time this function runs at, neither to
			// the time the advance started from nor to the time it is heading to.
			d.AfterFunc(5*time.Millisecond, func() { v = v*10 + 3; actual.Store(v) })
		})

		h.Advance(t, 20*time.Millisecond)
		assert.Equal(t, int32(1234), actual.Load())
	})

	run(t, setup, "afterFunc after time advanced uses relative delay", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		var actual atomic.Bool

		h.Advance(t, 50*time.Millisecond)
		d.AfterFunc(10*time.Millisecond, func() {
			actual.Store(true)
		})

		h.Advance(t, 59*time.Millisecond)
		assert.False(t, actual.Load(), "should not fire before relative delay")

		h.Advance(t, 60*time.Millisecond)
		assert.True(t, actual.Load(), "should fire at relative delay")
	})
}

func testTimerStop(t *testing.T, setup SetupFunc) {
	run(t, setup, "after execution", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		var actual atomic.Int32
		timer := d.AfterFunc(1*time.Millisecond, func() {
			actual.Add(1)
		})
		h.Advance(t, 1*time.Millisecond)
		assert.False(t, timer.Stop(), "Stop() should return false when stopping after execution")
		assert.Equal(t, int32(1), actual.Load(), "function should execute exactly once")
	})

	run(t, setup, "multiple calls", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		var actual atomic.Bool
		timer := d.AfterFunc(20*time.Millisecond, func() {
			actual.Store(true)
		})
		assert.True(t, timer.Stop(), "first Stop() should return true")
		assert.False(t, timer.Stop(), "second Stop() should return false")
		assert.False(t, timer.Stop(), "third Stop() should return false")
		h.Advance(t, 50*time.Millisecond)
		assert.False(t, actual.Load(), "function should not execute after being stopped")
	})

	run(t, setup, "stop from callback", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		var targetExecuted atomic.Bool
		var stopResult atomic.Bool
		target := d.AfterFunc(500*time.Millisecond, func() {
			targetExecuted.Store(true)
		})
		d.AfterFunc(100*time.Millisecond, func() {
			stopResult.Store(target.Stop())
		})
		h.Advance(t, 500*time.Millisecond)
		assert.True(t, stopResult.Load(), "Stop() from callback should return true")
		assert.False(t, targetExecuted.Load(), "stopped timer should not execute")
	})

	run(t, setup, "stop from callback of a timer due at the same time", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		// Which of two timers with the same delay runs first is unspecified, so each
		// stops the other: the one that runs finds the other's delay elapsed but its
		// function not yet run, which Stop still prevents. Both are scheduled from a
		// function so that their handles are written before either callback reads them.
		var ran atomic.Int32
		var stopped atomic.Bool
		d.AfterFunc(0, func() {
			var a, b task.Timer
			a = d.AfterFunc(10*time.Millisecond, func() { ran.Add(1); stopped.Store(b.Stop()) })
			b = d.AfterFunc(10*time.Millisecond, func() { ran.Add(1); stopped.Store(a.Stop()) })
		})
		h.Advance(t, 10*time.Millisecond)
		assert.Equal(t, int32(1), ran.Load(), "only the first of the two to run should run")
		assert.True(t, stopped.Load(), "Stop should report it prevented the other, whose delay had elapsed")
		h.Advance(t, 50*time.Millisecond)
		assert.Equal(t, int32(1), ran.Load(), "the stopped function never runs")
	})

	run(t, setup, "stop from its own callback", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		// The timer is scheduled from a function so that its handle is written
		// before the callback reads it.
		var timer task.Timer
		var ran atomic.Int32
		var stopped atomic.Bool
		d.AfterFunc(0, func() {
			timer = d.AfterFunc(10*time.Millisecond, func() { ran.Add(1); stopped.Store(timer.Stop()) })
		})
		h.Advance(t, 10*time.Millisecond)
		assert.Equal(t, int32(1), ran.Load())
		assert.False(t, stopped.Load(), "Stop from the running function reports it prevented nothing")
	})

	run(t, setup, "stopping one timer does not affect others", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		const (
			bitA = 1 << iota
			bitB
			bitC
		)
		var v int32
		var actual atomic.Int32
		d.AfterFunc(10*time.Millisecond, func() { v |= bitA; actual.Store(v) })
		timerB := d.AfterFunc(20*time.Millisecond, func() { v |= bitB; actual.Store(v) })
		d.AfterFunc(30*time.Millisecond, func() { v |= bitC; actual.Store(v) })

		assert.True(t, timerB.Stop(), "Stop() should return true for pending timer")
		h.Advance(t, 30*time.Millisecond)
		assert.Equal(t, int32(bitA|bitC), actual.Load(), "A and C should execute, B should be stopped")
	})

	run(t, setup, "concurrent stop", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		const numGoroutines = 100
		var actual atomic.Bool

		timer := d.AfterFunc(20*time.Millisecond, func() {
			actual.Store(true)
		})

		var wg sync.WaitGroup
		var successfulStops atomic.Int32
		for range numGoroutines {
			wg.Go(func() {
				if timer.Stop() {
					successfulStops.Add(1)
				}
			})
		}
		wg.Wait()

		assert.Equal(t, int32(1), successfulStops.Load(), "only one Stop() should succeed")
		h.Advance(t, 100*time.Millisecond)
		assert.False(t, actual.Load(), "function should not execute after being stopped")
	})

	run(t, setup, "concurrent AfterFunc and half stop", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		const numGoroutines = 1000
		executedCount := 0
		var actual atomic.Int32

		var wg sync.WaitGroup
		for i := range numGoroutines {
			wg.Go(func() {
				timer := d.AfterFunc(10*time.Millisecond, func() {
					executedCount++
					actual.Store(int32(executedCount))
				})
				if i%2 == 0 {
					timer.Stop()
				}
			})
		}
		wg.Wait()

		h.Advance(t, 10*time.Millisecond)
		assert.Equal(t, int32(numGoroutines/2), actual.Load(), "half of tasks should execute")
	})

	run(t, setup, "racing the firing, Stop reports true exactly when the function does not run", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		// Stop races the firing of a zero-delay timer. The yield in between lets
		// the firing get ahead, including onto a function already handed to the
		// dispatcher but not yet run. Which side wins is up to the scheduler and
		// one outcome can be rare, so the race repeats until both occurred, within
		// a bound that a dispatcher producing only one outcome still passes.
		var sawStopped, sawRan bool
		for i := 0; i < 2000 && !(sawStopped && sawRan); i++ {
			var ran atomic.Bool
			timer := d.AfterFunc(0, func() { ran.Store(true) })
			runtime.Gosched()
			stopped := timer.Stop()
			h.Advance(t, 0)
			if stopped {
				sawStopped = true
				assert.False(t, ran.Load(), "a function Stop reports it prevented never runs")
			} else {
				sawRan = true
				assert.True(t, ran.Load(), "Stop reports false only for a function that runs")
			}
		}
	})

	run(t, setup, "from a running function on a timer whose delay elapsed meanwhile", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		release := make(chan struct{})
		var target task.Timer
		var stopResult, ran atomic.Bool
		d.AfterFunc(0, func() {
			<-release
			stopResult.Store(target.Stop())
		})
		target = d.AfterFunc(1*time.Millisecond, func() { ran.Store(true) })

		// The sleep lets the timer's delay elapse while the function above holds
		// the dispatcher. Unlike Advance, the test does not wait for the bubble to
		// settle before it releases the function: a dispatcher that serializes with
		// a lock parks the elapsed timer's goroutine on that lock, which synctest
		// does not count as durably blocked.
		time.Sleep(1 * time.Millisecond)
		close(release)

		h.Advance(t, 1*time.Millisecond)
		assert.True(t, stopResult.Load(), "Stop reports true while the elapsed function waits for the dispatcher")
		assert.False(t, ran.Load(), "the stopped function never runs")
	})
}

// panicCases are the values the panicking functions under test panic with, the
// message the dispatcher reports for each, and the value recover yields for it:
// the value itself, except that panic(nil) recovers as a *runtime.PanicNilError.
var panicCases = []struct {
	name      string
	value     any
	recovered any
	message   string
}{
	{"string", "boom", "boom", "panic: boom"},
	{"int", 42, 42, "panic: 42"},
	// PanicNilError's message differs between Go releases; match the part they share.
	{"nil", nil, &runtime.PanicNilError{}, "panic called with nil argument"},
}

// panicValue returns the value f panics with, and fails the test if f returns.
func panicValue(t *testing.T, f func()) (recovered any) {
	t.Helper()
	defer func() { recovered = recover() }()
	f()
	t.Error("expected a panic")
	return nil
}

func testInvokeFunc(t *testing.T, setup SetupFunc) {
	run(t, setup, "executes the submitted functions once each, in submission order", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		// Each function appends its digit, so the value reads as the order they ran in.
		var v int32
		var actual atomic.Int32
		first := d.InvokeFunc(func() { v = v*10 + 1; actual.Store(v) })
		second := d.InvokeFunc(func() { v = v*10 + 2; actual.Store(v) })
		h.Advance(t, 0)
		assert.NoError(t, first.Wait(t.Context()))
		assert.NoError(t, second.Wait(t.Context()))
		assert.Equal(t, int32(12), actual.Load())
		h.Advance(t, 50*time.Millisecond)
		assert.Equal(t, int32(12), actual.Load(), "each function should execute exactly once")
	})

	run(t, setup, "follow-up work scheduled with AfterFunc(0) runs after the function returns", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		// Each function appends its digit, so the value reads as the order they ran in.
		var v int32
		var actual atomic.Int32
		invoked := d.InvokeFunc(func() {
			d.AfterFunc(0, func() { v = v*10 + 2; actual.Store(v) })
			v = v*10 + 1
			actual.Store(v)
		})
		h.Advance(t, 0)
		assert.NoError(t, invoked.Wait(t.Context()))
		assert.Equal(t, int32(12), actual.Load())
	})

	run(t, setup, "concurrent InvokeFunc runs every submission serialized", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		const numGoroutines = 1000
		var counter int
		var actual atomic.Int32
		errs := make([]error, numGoroutines)

		var wg sync.WaitGroup
		for i := range numGoroutines {
			wg.Go(func() {
				errs[i] = d.InvokeFunc(func() {
					counter++
					actual.Store(int32(counter))
				}).Wait(t.Context())
			})
		}
		wg.Wait()

		h.Advance(t, 0)
		assert.Equal(t, int32(numGoroutines), actual.Load(), "all concurrently submitted functions run, serialized")
		for _, err := range errs {
			assert.NoError(t, err)
		}
	})

	run(t, setup, "concurrent AfterFunc and InvokeFunc run serialized against each other", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		const numGoroutines = 1000
		var counter int
		var actual atomic.Int32
		errs := make([]error, numGoroutines/2)

		var wg sync.WaitGroup
		for i := range numGoroutines {
			wg.Go(func() {
				f := func() {
					counter++
					actual.Store(int32(counter))
				}
				if i%2 == 0 {
					d.AfterFunc(0, f)
				} else {
					errs[i/2] = d.InvokeFunc(f).Wait(t.Context())
				}
			})
		}
		wg.Wait()

		h.Advance(t, 0)
		assert.Equal(t, int32(numGoroutines), actual.Load(), "every function runs, serialized across AfterFunc and InvokeFunc")
		for _, err := range errs {
			assert.NoError(t, err)
		}
	})

	run(t, setup, "Task.Wait reports the settled result even when ctx is already done", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		// A Wait that selects on ctx and the result at once picks at random when
		// both are ready, so a regression shows in about half of the runs: repeat.
		for range 30 {
			invoked := d.InvokeFunc(func() {})
			h.Advance(t, 0)
			// The function completed, so ctx was not done first: Wait must return its
			// result, not the ctx cause.
			assert.NoError(t, invoked.Wait(ctx))
		}
	})

	for _, tc := range panicCases {
		run(t, setup, "panic in f is reported by the dispatcher and Task.Wait re-panics with the "+tc.name+" value", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
			invoked := d.InvokeFunc(func() { panic(tc.value) })
			assert.ErrorContains(t, h.AdvanceTo(h.Start), tc.message)
			assert.Equal(t, tc.recovered, panicValue(t, func() { _ = invoked.Wait(t.Context()) }))
			assert.Equal(t, tc.recovered, panicValue(t, func() { _ = invoked.Wait(t.Context()) }), "Wait re-panics on every call")
		})
	}

	run(t, setup, "after f panicked, pending and subsequent submissions never run", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		d.InvokeFunc(func() { panic("boom") })
		var pendingRan atomic.Bool
		pending := d.InvokeFunc(func() { pendingRan.Store(true) })
		pendingTimer := d.AfterFunc(5*time.Millisecond, func() { pendingRan.Store(true) })
		assert.ErrorContains(t, h.AdvanceTo(h.Start), "panic: boom")

		assert.ErrorIs(t, pending.Wait(t.Context()), task.ErrCanceled)

		var laterRan atomic.Bool
		err := d.InvokeFunc(func() { laterRan.Store(true) }).Wait(t.Context())
		assert.ErrorIs(t, err, task.ErrCanceled)
		_ = h.AdvanceTo(h.Start.Add(50 * time.Millisecond))
		assert.True(t, pendingTimer.Stop(), "Stop on a timer prevented by the stop reports true")
		assert.False(t, pendingRan.Load(), "pending functions should not run after the dispatcher stopped")
		assert.False(t, laterRan.Load(), "function submitted after the stop should not run")
	})

	run(t, setup, "after f panicked, concurrent submissions settle as ErrCanceled", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		d.InvokeFunc(func() { panic("boom") })
		assert.ErrorContains(t, h.AdvanceTo(h.Start), "panic: boom")

		const numGoroutines = 200
		var wg sync.WaitGroup
		var ran atomic.Bool
		errs := make([]error, numGoroutines)
		for i := range errs {
			wg.Go(func() {
				errs[i] = d.InvokeFunc(func() { ran.Store(true) }).Wait(t.Context())
			})
		}
		wg.Wait()

		for _, err := range errs {
			assert.ErrorIs(t, err, task.ErrCanceled)
		}
		assert.False(t, ran.Load(), "function should not run on a stopped dispatcher")
	})
	// The case where Wait returns the context cause before f runs applies only to
	// asynchronous implementations (a synchronous dispatcher always completes f
	// first). See the task/queue tests.
}

func testPanic(t *testing.T, setup SetupFunc) {
	run(t, setup, "subsequent task not executed", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		var subsequent atomic.Bool
		d.AfterFunc(100*time.Millisecond, func() {
			panic("boom")
		})
		d.AfterFunc(200*time.Millisecond, func() {
			subsequent.Store(true)
		})
		err := h.AdvanceTo(h.Start.Add(200 * time.Millisecond))
		assert.ErrorContains(t, err, "panic: boom")
		assert.False(t, subsequent.Load(), "subsequent task should not execute after panic")
	})

	run(t, setup, "only first panic reported", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		d.AfterFunc(5*time.Millisecond, func() {
			panic("first panic")
		})
		d.AfterFunc(10*time.Millisecond, func() {
			panic("second panic")
		})
		err := h.AdvanceTo(h.Start.Add(10 * time.Millisecond))
		require.ErrorContains(t, err, "panic: first panic")
		assert.NotContains(t, err.Error(), "second panic")
	})

	run(t, setup, "Stop reports false for the timer whose function panicked", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		timer := d.AfterFunc(5*time.Millisecond, func() { panic("boom") })
		assert.ErrorContains(t, h.AdvanceTo(h.Start.Add(5*time.Millisecond)), "panic: boom")
		assert.False(t, timer.Stop(), "the function ran, so Stop prevented nothing")
	})

	run(t, setup, "Stop reports true for a timer still pending when a panic stops the dispatcher", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		var ran atomic.Bool
		pending := d.AfterFunc(50*time.Millisecond, func() { ran.Store(true) })
		d.AfterFunc(5*time.Millisecond, func() { panic("boom") })

		assert.ErrorContains(t, h.AdvanceTo(h.Start.Add(50*time.Millisecond)), "panic: boom")
		// The function never ran, so Stop reports it prevented execution.
		assert.True(t, pending.Stop(), "Stop on a timer prevented by the stop reports true")
		assert.False(t, pending.Stop(), "second Stop reports it was already stopped")
		assert.False(t, ran.Load(), "the prevented function never runs")
	})

	run(t, setup, "AfterFunc after a panic stopped the dispatcher never fires and Stop reports it prevented execution", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		d.AfterFunc(5*time.Millisecond, func() { panic("boom") })
		assert.ErrorContains(t, h.AdvanceTo(h.Start.Add(5*time.Millisecond)), "panic: boom")

		var ran atomic.Bool
		timer := d.AfterFunc(1*time.Millisecond, func() { ran.Store(true) })
		assert.True(t, timer.Stop(), "Stop on a timer the stopped dispatcher would never run reports true")
		assert.False(t, timer.Stop(), "second Stop reports it was already stopped")

		d.AfterFunc(1*time.Millisecond, func() { ran.Store(true) })
		_ = h.AdvanceTo(h.Start.Add(time.Hour))
		assert.False(t, ran.Load(), "no function submitted after the stop runs")
	})

	run(t, setup, "InvokeFunc after a panic stopped the dispatcher settles as ErrCanceled", func(t *testing.T, d task.Dispatcher, h *TestHelper) {
		d.AfterFunc(5*time.Millisecond, func() { panic("boom") })
		assert.ErrorContains(t, h.AdvanceTo(h.Start.Add(5*time.Millisecond)), "panic: boom")

		var ran atomic.Bool
		assert.ErrorIs(t, d.InvokeFunc(func() { ran.Store(true) }).Wait(t.Context()), task.ErrCanceled)
		_ = h.AdvanceTo(h.Start.Add(50 * time.Millisecond))
		assert.False(t, ran.Load(), "function submitted after the stop should not run")
	})
}
