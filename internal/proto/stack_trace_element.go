package proto

type StackTraceElement struct {
	className  string
	methodName string
	fileName   string
	lineNumber int32
}

func NewStackTraceElement(className, methodName, fileName string, lineNumber int32) StackTraceElement {
	return StackTraceElement{className, methodName, fileName, lineNumber}
}

func (s StackTraceElement) GetClassName() string {
	return s.className
}

func (s StackTraceElement) GetMethodName() string {
	return s.methodName
}

func (s StackTraceElement) GetFileName() string {
	return s.fileName
}

func (s StackTraceElement) GetLineNumber() int32 {
	return s.lineNumber
}
