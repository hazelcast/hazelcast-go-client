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

func (s StackTraceElement) ClassName() string {
	return s.className
}

func (s StackTraceElement) MethodName() string {
	return s.methodName
}

func (s StackTraceElement) FileName() string {
	return s.fileName
}

func (s StackTraceElement) LineNumber() int32 {
	return s.lineNumber
}
