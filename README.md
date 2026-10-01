# serialdispatch

Go library that serializes function execution. A `Dispatcher` runs every function submitted to it one at a time, so the functions can share state without further locking. Each submission returns a handle: a `Timer` to cancel a delayed function, or a `Task` to await an invoked one.

## Installation

```bash
go get github.com/raiich/serialdispatch
```

Requires Go 1.25 or later.

## Usage

`task.Dispatcher` has two methods:

- `AfterFunc(d, f)` schedules `f` after the delay `d` and returns a `Timer`. Its `Stop` cancels `f` and reports true as long as `f` has not started, even after the delay elapsed.
- `InvokeFunc(f)` submits `f` and returns a `Task`. Its `Wait` blocks until `f` finishes.

Functions run in the order their delays elapse. If a function panics, the dispatcher stops: no remaining function runs, and later submissions settle as `task.ErrCanceled`.

A function the dispatcher runs must not call `InvokeFunc` or `Task.Wait` on the same dispatcher, which may deadlock. It schedules follow-up work with `AfterFunc(0, f)`.

### queue

`queue.Dispatcher` runs the functions on the goroutine that calls `Serve`. Cancel the context to stop it; a function that cancels it is the last to run. `Serve` returns the error that stopped it, including the panic of a function. See the [package example](task/queue/example_test.go).

### mutex

`mutex.Dispatcher` has no goroutine of its own. `InvokeFunc` runs the function in the caller's goroutine and `AfterFunc` in the timer's goroutine, serialized by a mutex. A panic is reported on the channel that `Err` returns. See the [package example](task/mutex/example_test.go).

## Implementing a Dispatcher

`tasktest.TestDispatcher` runs the conformance tests every implementation is expected to pass: execution order, `Stop` semantics, `Task.Wait` results and panic handling. The tests run inside a `testing/synctest` bubble, so the setup advances time by sleeping. See the test of the mutex package for a complete setup. The case only an asynchronous dispatcher reaches, `Task.Wait` returning the context cause before the function runs, is tested per implementation.

```go
func TestDispatcher(t *testing.T) {
	tasktest.TestDispatcher(t, func(tb testing.TB) (task.Dispatcher, *tasktest.TestHelper) {
		// Create the dispatcher. TestHelper.AdvanceTo moves time to the given
		// instant and returns the error the dispatcher reported, if any.
	})
}
```

`tasktest.BenchmarkDispatcher` runs the benchmarks shared by the implementations: the `InvokeFunc` round trip and pipelined submission, and the `AfterFunc` fire and stop paths. They measure real time, so the setup returns a running dispatcher outside any synctest bubble. Compare revisions, or the implementations within one run, with [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```bash
go test ./... -run '^$' -bench . -count 10 > new.txt
benchstat old.txt new.txt    # revisions
benchstat -col pkg new.txt   # implementations
```

## Packages

- `task`: the `Dispatcher` interface and the `Timer` and `Task` handles
- `task/queue`: dispatcher served by a run loop
- `task/mutex`: dispatcher serialized by a mutex, with no goroutine of its own
- `task/tasktest`: conformance tests and benchmarks for implementations

## License

MIT. See [LICENSE](LICENSE).
