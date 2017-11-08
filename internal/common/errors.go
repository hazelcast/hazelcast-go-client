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
type HazelcastIllegalStateError struct {
	*HazelcastErrorType
}
type HazelcastTargetDisconnectedError struct {
	*HazelcastErrorType
}
type HazelcastInstanceNotActiveError struct {
	*HazelcastErrorType
}
type HazelcastKeyError struct {
	*HazelcastErrorType
}

func NewHazelcastKeyError(message string, cause error) *HazelcastKeyError {
	return &HazelcastKeyError{&HazelcastErrorType{message: message, cause: cause}}
}
func NewHazelcastClientNotActiveError(message string, cause error) *HazelcastClientNotActiveError {
	return &HazelcastClientNotActiveError{&HazelcastErrorType{message: message, cause: cause}}
}
func NewHazelcastErrorType(message string, cause error) *HazelcastErrorType {
	return &HazelcastErrorType{message: message, cause: cause}
}
func NewHazelcastIllegalStateError(message string, cause error) *HazelcastIllegalStateError {
	return &HazelcastIllegalStateError{&HazelcastErrorType{message: message, cause: cause}}
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
