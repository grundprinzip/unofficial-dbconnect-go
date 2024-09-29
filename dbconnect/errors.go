package dbconnect

import (
	"errors"
	"fmt"
)

type wrappedError struct {
	errorType error
	cause     error
}

func (w *wrappedError) Unwrap() []error {
	return []error{w.errorType, w.cause}
}

func (w *wrappedError) Error() string {
	return fmt.Sprintf("%s: %s", w.errorType, w.cause)
}

// WithType wraps an error with a type that can later be checked using `errors.Is`
func WithType(err error, errType errorType) error {
	return &wrappedError{cause: err, errorType: errType}
}

type errorType error

var InvalidConfigurationError = errorType(errors.New("invalid configuration"))
var ConnectionError = errorType(errors.New("connection error"))
