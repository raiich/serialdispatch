package mutex_test

import (
	"fmt"
	"log"
	"time"

	"github.com/raiich/serialdispatch/task/mutex"
)

func Example() {
	d := mutex.NewDispatcher()

	var counter int // touched only by the functions d runs
	fired := make(chan struct{})
	d.AfterFunc(time.Millisecond, func() {
		counter++
		close(fired)
	})
	d.InvokeFunc(func() { counter++ }) // returns once counter++ ran
	<-fired

	// Reading counter goes through the dispatcher as well.
	d.InvokeFunc(func() { fmt.Println(counter) })

	select {
	case err := <-d.Err():
		log.Fatal(err)
	default:
	}
	// Output: 2
}
