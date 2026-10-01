package tasktest

import (
	"testing"
	"time"

	"github.com/raiich/serialdispatch/task"
)

// BenchmarkSetupFunc creates a fresh, running Dispatcher for a single benchmark.
// The benchmarks measure real time, so it is called outside any synctest
// bubble. It may register cleanups via b.Cleanup, which run after the benchmark
// has waited for every function it submitted.
type BenchmarkSetupFunc func(b *testing.B) task.Dispatcher

// BenchmarkDispatcher runs the benchmarks shared by every [task.Dispatcher]
// implementation, one fresh dispatcher from setup per benchmark:
//
//   - InvokeFunc/roundtrip: submit a function and wait for it, per iteration.
//   - InvokeFunc/pipelined: submit functions without waiting; the queue depth of
//     the implementation, if any, is part of the cost.
//   - AfterFunc/fire: schedule a function with zero delay and wait until it ran.
//   - AfterFunc/stop: schedule a function far in the future and stop it.
//
// Each Dispatcher implementation should call this from its own package
// benchmark, passing a BenchmarkSetupFunc.
func BenchmarkDispatcher(b *testing.B, setup BenchmarkSetupFunc) {
	b.Helper()
	b.Run("InvokeFunc", func(b *testing.B) {
		benchmarkInvokeFunc(b, setup)
	})
	b.Run("AfterFunc", func(b *testing.B) {
		benchmarkAfterFunc(b, setup)
	})
}

// The functions submitted below are created once, outside the loop, so the
// allocations reported per iteration are the dispatcher's own.

func benchmarkInvokeFunc(b *testing.B, setup BenchmarkSetupFunc) {
	b.Run("roundtrip", func(b *testing.B) {
		d := setup(b)
		f := func() {}
		b.ReportAllocs()
		for b.Loop() {
			if err := d.InvokeFunc(f).Wait(b.Context()); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("pipelined", func(b *testing.B) {
		d := setup(b)
		f := func() {}
		var last task.Task
		b.ReportAllocs()
		for b.Loop() {
			last = d.InvokeFunc(f)
		}
		// Functions run in submission order, so once the last one settles every
		// submission has run and the cleanup finds nothing pending.
		if err := last.Wait(b.Context()); err != nil {
			b.Fatal(err)
		}
	})
}

func benchmarkAfterFunc(b *testing.B, setup BenchmarkSetupFunc) {
	b.Run("fire", func(b *testing.B) {
		d := setup(b)
		ran := make(chan struct{})
		f := func() { ran <- struct{}{} }
		b.ReportAllocs()
		// Waiting for each firing keeps the timers' goroutines from piling up
		// ahead of the dispatcher.
		for b.Loop() {
			d.AfterFunc(0, f)
			<-ran
		}
	})

	b.Run("stop", func(b *testing.B) {
		d := setup(b)
		f := func() {}
		b.ReportAllocs()
		for b.Loop() {
			if !d.AfterFunc(time.Hour, f).Stop() {
				b.Fatal("Stop reported false for a pending timer")
			}
		}
	})
}
