package queue_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/raiich/serialdispatch/task/queue"
)

func Example() {
	d := queue.NewDispatcher()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := d.Serve(ctx); !errors.Is(err, context.Canceled) {
			log.Print(err)
		}
	}()

	var counter int // touched only by the functions d runs
	fired := make(chan struct{})
	d.AfterFunc(time.Millisecond, func() {
		counter++
		close(fired)
	})
	if err := d.InvokeFunc(func() { counter++ }).Wait(ctx); err != nil {
		log.Fatal(err)
	}
	<-fired

	// Reading counter goes through the dispatcher as well.
	if err := d.InvokeFunc(func() { fmt.Println(counter) }).Wait(ctx); err != nil {
		log.Fatal(err)
	}
	// Output: 2
}
