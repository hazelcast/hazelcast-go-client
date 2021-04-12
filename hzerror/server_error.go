package hzerror

// StackTraceElement contains stacktrace information for server side exception.
type StackTraceElement interface {
	// DeclaringClass returns the fully qualified name of the class containing
	// the execution point represented by the stack trace element.
	ClassName() string

	// MethodName returns the name of the method containing the execution point
	// represented by this stack trace element.
	MethodName() string

	// FileName returns the name of the file containing the execution point
	// represented by the stack trace element, or nil if
	// this information is unavailable.
	FileName() string

	// LineNumber returns the line number of the source line containing the
	// execution point represented by this stack trace element, or
	// a negative number if this information is unavailable. A value
	// of -2 indicates that the method containing the execution point
	// is a native method.
	LineNumber() int32
}

// ServerError contains error information that occurred in the server.
type ServerError interface {
	// ErrorCode returns the error code.
	ErrorCode() int32

	// ClassName returns the class name where error occurred.
	ClassName() string

	// Message returns the error message.
	Message() string

	// StackTrace returns a slice of StackTraceElement.
	StackTrace() []StackTraceElement

	// CauseErrorCode returns the cause error code.
	CauseErrorCode() int32

	// CauseClassName returns the cause class name.
	CauseClassName() string
}
