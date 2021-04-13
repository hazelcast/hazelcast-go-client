package hzerror

type ServerErrorImpl struct {
	errorCode      int32
	className      string
	message        string
	stackTrace     []StackTraceElement
	causeErrorCode int32
	causeClassName string
}

// NewServerErrorImpl
// experimental
func NewServerErrorImpl(errorCode int32, className string, message string, stackTrace []StackTraceElement, causeErrorCode int32, causeClassName string) ServerErrorImpl {
	return ServerErrorImpl{
		errorCode:      errorCode,
		className:      className,
		message:        message,
		stackTrace:     stackTrace,
		causeErrorCode: causeErrorCode,
		causeClassName: causeClassName,
	}
}

func (e *ServerErrorImpl) Error() string {
	return e.message
}

func (e *ServerErrorImpl) ErrorCode() int32 {
	return e.errorCode
}

func (e *ServerErrorImpl) ClassName() string {
	return e.className
}

func (e *ServerErrorImpl) Message() string {
	return e.message
}

func (e *ServerErrorImpl) StackTrace() []StackTraceElement {
	stackTrace := make([]StackTraceElement, len(e.stackTrace))
	for i, v := range e.stackTrace {
		stackTrace[i] = v
	}
	return stackTrace
}

func (e *ServerErrorImpl) CauseErrorCode() int32 {
	return e.causeErrorCode
}

func (e *ServerErrorImpl) CauseClassName() string {
	return e.causeClassName
}
