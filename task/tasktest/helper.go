package tasktest

import (
	"testing"
	"testing/synctest"
)

// WithSyncTest wraps f with synctest.Test and reports a bubble deadlock, which
// synctest raises as a panic, as a failure of this test alone so the remaining
// tests still run.
func WithSyncTest(f func(t *testing.T)) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("synctest: %v", r)
			}
		}()
		synctest.Test(t, f)
	}
}
