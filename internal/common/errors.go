package common

type HazelcastError interface {
	Error() string
	Cause() error
}

type HazelcastErrorType struct {
	message string
	cause   error
}

func (e *HazelcastErrorType) Error() string {
	return e.message
}

func (e *HazelcastErrorType) Cause() error {
	return e.cause
}

type HazelcastEOFError struct {
	*HazelcastErrorType
}

type HazelcastSerializationError struct {
	*HazelcastErrorType
}

type HazelcastAuthenticationError struct {
	*HazelcastErrorType
}

type HazelcastIllegalArgumentError struct {
	*HazelcastErrorType
}
type HazelcastTargetDisconnectedError struct {
	*HazelcastErrorType
}

func NewHazelcastErrorType(message string, cause error) *HazelcastErrorType {
	return &HazelcastErrorType{message: message, cause: cause}
}
func NewHazelcastTargetDisconnectedError(message string, cause error) *HazelcastTargetDisconnectedError {
	return &HazelcastTargetDisconnectedError{&HazelcastErrorType{message: message, cause: cause}}
}
func NewHazelcastEOFError(message string, cause error) *HazelcastEOFError {
	return &HazelcastEOFError{&HazelcastErrorType{message, cause}}
}

func NewHazelcastSerializationError(message string, cause error) *HazelcastSerializationError {
	return &HazelcastSerializationError{&HazelcastErrorType{message, cause}}
}

func NewHazelcastIllegalArgumentError(message string, cause error) *HazelcastIllegalArgumentError {
	return &HazelcastIllegalArgumentError{&HazelcastErrorType{message, cause}}
}

func NewHazelcastAuthenticationError(message string, cause error) *HazelcastAuthenticationError {
	return &HazelcastAuthenticationError{&HazelcastErrorType{message, cause}}
}
