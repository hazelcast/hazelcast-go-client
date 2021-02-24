package proto

type ErrorHolder struct {
	errorCode          int32
	className          string
	message            string
	stackTraceElements []StackTraceElement
}

func NewErrorHolder(errorCode int32, className, message string, stackTraceElements []StackTraceElement) ErrorHolder {
	return ErrorHolder{errorCode, className, message, stackTraceElements}
}

func (e ErrorHolder) ErrorCode() int32 {
	return e.errorCode
}

func (e ErrorHolder) ClassName() string {
	return e.className
}

func (e ErrorHolder) Message() string {
	return e.message
}

func (e ErrorHolder) StackTraceElements() []StackTraceElement {
	return e.stackTraceElements
}
