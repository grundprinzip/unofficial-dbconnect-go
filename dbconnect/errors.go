package dbconnect

import (
	"fmt"
	"github.com/go-errors/errors"
	"io"
)

type wrappedError struct {
	errorType error
	cause     *errors.Error
}

func (w *wrappedError) Unwrap() []error {
	return []error{w.errorType, w.cause}
}

func (w *wrappedError) Error() string {
	return fmt.Sprintf("%w", w)
}

// Format formats the error, supporting both short forms (v, s, q) and verbose form (+v)
func (w *wrappedError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			_, _ = io.WriteString(s, "[sparkerror] ")
			_, _ = io.WriteString(s, fmt.Sprintf("Error Type: %s\n", w.errorType.Error()))
			_, _ = io.WriteString(s, fmt.Sprintf("Error Cause: %s\n%s", w.cause.Err.Error(), w.cause.Stack()))
			return
		}
		fallthrough
	case 's':
		_, _ = io.WriteString(s, fmt.Sprintf("%s: %s", w.errorType, w.cause))
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", w.errorType.Error())
	}
}

// WithType wraps an error with a type that can later be checked using `errors.Is`
func WithType(err error, errType errorType) error {
	return &wrappedError{cause: errors.Wrap(err, 1), errorType: errType}
}

type errorType error

var InvalidConfigurationError = errorType(errors.New("invalid configuration"))
var ConnectionError = errorType(errors.New("connection error"))
