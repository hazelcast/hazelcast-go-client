package hzerror

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/hzerror"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

func CreateHazelcastError(err *ServerErrorImpl) hzerror.HazelcastError {
	return createHazelcastError(err)
}

func createHazelcastError(err *ServerErrorImpl) hzerror.HazelcastError {
	stackTrace := ""
	for _, trace := range err.StackTrace() {
		stackTrace += fmt.Sprintf("\n %s.%s(%s:%d)", trace.ClassName(), trace.MethodName(), trace.FileName(),
			trace.LineNumber())
	}
	message := fmt.Sprintf("got exception from server:\n %s: %s\n %s", err.ClassName(), err.Message(), stackTrace)
	switch bufutil.ErrorCode(err.ErrorCode()) {
	case bufutil.ErrorCodeAuthentication:
		return hzerror.NewHazelcastAuthenticationError(message, err)
	case bufutil.ErrorCodeHazelcastInstanceNotActive:
		return hzerror.NewHazelcastInstanceNotActiveError(message, err)
	case bufutil.ErrorCodeHazelcastSerialization:
		return hzerror.NewHazelcastSerializationError(message, err)
	case bufutil.ErrorCodeTargetDisconnected:
		return hzerror.NewHazelcastTargetDisconnectedError(message, err)
	case bufutil.ErrorCodeTargetNotMember:
		return hzerror.NewHazelcastTargetNotMemberError(message, err)
	case bufutil.ErrorCodeUnsupportedOperation:
		return hzerror.NewHazelcastUnsupportedOperationError(message, err)
	case bufutil.ErrorCodeConsistencyLostException:
		return hzerror.NewHazelcastConsistencyLostError(message, err)
	case bufutil.ErrorCodeIllegalArgument:
		return hzerror.NewHazelcastIllegalArgumentError(message, err)
	}

	return hzerror.NewHazelcastErrorType(message, err)
}
