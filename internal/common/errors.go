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
type HazelcastClientNotActiveError struct {
	*HazelcastErrorType
}
type HazelcastTargetNotMemberError struct {
	*HazelcastErrorType
}
type HazelcastTargetDisconnectedError struct {
	*HazelcastErrorType
}
type HazelcastInstanceNotActiveError struct {
	*HazelcastErrorType
}

func NewHazelcastClientNotActiveError(message string, cause error) *HazelcastClientNotActiveError {
	return &HazelcastClientNotActiveError{&HazelcastErrorType{message: message, cause: cause}}
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
func NewHazelcastTargetDisconnectedError(message string, cause error) *HazelcastTargetDisconnectedError {
	return &HazelcastTargetDisconnectedError{&HazelcastErrorType{message: message, cause: cause}}
}
