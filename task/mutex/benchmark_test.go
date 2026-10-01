package mutex

import (
	"testing"

	"github.com/raiich/serialdispatch/task"
	"github.com/raiich/serialdispatch/task/tasktest"
)

func BenchmarkDispatcher(b *testing.B) {
	tasktest.BenchmarkDispatcher(b, func(*testing.B) task.Dispatcher {
		return NewDispatcher()
	})
}
