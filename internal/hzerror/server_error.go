package hzerror

import (
	pubhzerror "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hzerror"
)

type ServerErrorImpl struct {
	errorCode      int32
	className      string
	message        string
	stackTrace     []pubhzerror.StackTraceElement
	causeErrorCode int32
	causeClassName string
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

func (e *ServerErrorImpl) StackTrace() []pubhzerror.StackTraceElement {
	stackTrace := make([]pubhzerror.StackTraceElement, len(e.stackTrace))
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
