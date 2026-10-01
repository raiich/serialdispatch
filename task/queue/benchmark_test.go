package queue

import (
	"testing"

	"github.com/raiich/serialdispatch/task"
	"github.com/raiich/serialdispatch/task/tasktest"
)

func BenchmarkDispatcher(b *testing.B) {
	tasktest.BenchmarkDispatcher(b, func(b *testing.B) task.Dispatcher {
		dispatcher := NewDispatcher()
		stopped := make(chan struct{})
		// b.Context is canceled just before the cleanups run, which stops Serve.
		go func() {
			defer close(stopped)
			_ = dispatcher.Serve(b.Context())
		}()
		b.Cleanup(func() { <-stopped })
		return dispatcher
	})
}
