package task

// Timer represents a stoppable scheduled task, similar to [time.Timer].
//
// Timer is an alias of an interface literal so that it is a general-purpose
// timer type rather than one tied to this module: a Dispatcher that another
// module declares with the same literal is the identical type, and an
// implementation satisfies both without a shared import.
type Timer = interface {
	// Stop prevents the scheduled function from executing. It returns true if
	// the call successfully prevents execution, false if the function has
	// already started or Stop has already been called.
	//
	// Unlike [time.Timer.Stop], Stop returns true as long as the function has
	// not yet been started by the [Dispatcher], even if the timer's duration
	// has already elapsed. This means Stop called from within a dispatched
	// callback can still cancel a pending function.
	//
	// Stop is safe for concurrent use.
	Stop() bool
}

// Fails to compile if Timer becomes a named interface, which would break the
// cross-module identity above.
var _ func() Timer = func() interface{ Stop() bool } { return nil }
