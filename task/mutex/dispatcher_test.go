package mutex

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/raiich/serialdispatch/task"
	"github.com/raiich/serialdispatch/task/tasktest"
	"github.com/stretchr/testify/assert"
)

func TestDispatcher(t *testing.T) {
	tasktest.TestDispatcher(t, func(testing.TB) (task.Dispatcher, *tasktest.TestHelper) {
		dispatcher := NewDispatcher()
		return dispatcher, &tasktest.TestHelper{
			Start: time.Now(),
			AdvanceTo: func(to time.Time) error {
				if dur := time.Until(to); dur > 0 {
					time.Sleep(dur)
				}
				synctest.Wait()
				select {
				case err := <-dispatcher.Err():
					return err
				default:
					return nil
				}
			},
		}
	})
}

func TestDispatcher_InvokeFunc(t *testing.T) {
	t.Run("runs f before returning", func(t *testing.T) {
		dispatcher := NewDispatcher()
		ran := false
		dispatcher.InvokeFunc(func() { ran = true })
		assert.True(t, ran)
	})
}
