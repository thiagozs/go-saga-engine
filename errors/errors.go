package errors

type StageError struct {
	Retryable bool
	Reason    string
}

func (e *StageError) Error() string {
	return e.Reason
}

func Retryable(reason string) *StageError {
	return &StageError{
		Retryable: true,
		Reason:    reason,
	}
}

func Fatal(reason string) *StageError {
	return &StageError{
		Retryable: false,
		Reason:    reason,
	}
}
