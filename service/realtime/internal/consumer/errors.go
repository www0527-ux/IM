package consumer

import (
	"errors"
	"fmt"
)

// PermanentError marks a record that cannot be handled by repeating the same
// business operation. The reader must first preserve it in the DLQ.
type PermanentError struct {
	Reason string
	Err    error
}

// FatalError reports a broken consumer dependency or configuration. Repeating
// the record or sending it to the DLQ cannot repair the process.
type FatalError struct{ Err error }

func (e *FatalError) Error() string { return e.Err.Error() }
func (e *FatalError) Unwrap() error { return e.Err }

func (e *PermanentError) Error() string {
	if e.Err == nil {
		return e.Reason
	}
	return fmt.Sprintf("%s: %v", e.Reason, e.Err)
}

func (e *PermanentError) Unwrap() error { return e.Err }

func permanent(reason string, err error) error {
	return &PermanentError{Reason: reason, Err: err}
}

func asPermanent(err error) (*PermanentError, bool) {
	var target *PermanentError
	return target, errors.As(err, &target)
}

func asFatal(err error) bool {
	var target *FatalError
	return errors.As(err, &target)
}
