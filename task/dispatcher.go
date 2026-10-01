package task

import "time"

// Dispatcher defines the interface for scheduling delayed function execution.
//
// AfterFunc and InvokeFunc are safe for concurrent use from multiple goroutines.
type Dispatcher interface {
	// AfterFunc schedules f to be executed after duration d and returns a [Timer]
	// that can cancel the scheduled execution.
	// Zero or negative d causes immediate scheduling (equivalent to d == 0).
	//
	// Synchronization: all scheduled functions are executed serially.
	// No additional synchronization is needed within callbacks, and shared
	// variable access is safe across callbacks. Functions run in the order their
	// delays elapse; delays the dispatcher cannot tell apart run in unspecified order.
	//
	// Panic handling: if f panics, the panic is caught and all remaining
	// scheduled functions are not executed. How the panic value is
	// reported is implementation-dependent.
	//
	// Example:
	//
	//	var counter int
	//	d.AfterFunc(1*time.Millisecond, func() { counter += 2 })
	//	d.AfterFunc(1*time.Millisecond, func() { counter += 3 })
	//	// After execution: counter == 5 (no race conditions)
	AfterFunc(d time.Duration, f func()) Timer

	// InvokeFunc submits f for serialized execution and returns a [Task] to await
	// its completion. The Synchronization and Panic handling notes on AfterFunc
	// apply. For the Task's result — success, [ErrCanceled] if the dispatcher
	// stopped first, or a re-raised panic — see [Task.Wait].
	//
	// An implementation may run f synchronously in the caller's goroutine, so
	// calling InvokeFunc from within a dispatched function may deadlock.
	InvokeFunc(f func()) Task
}
