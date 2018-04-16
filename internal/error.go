// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

func CreateHazelcastError(err *protocol.Error) core.HazelcastError {
	stackTrace := ""
	for _, trace := range err.StackTrace() {
		stackTrace += fmt.Sprintf("\n %s.%s(%s:%d)", trace.DeclaringClass(), trace.MethodName(), trace.FileName(),
			trace.LineNumber())
	}
	message := fmt.Sprintf("got exception from server:\n %s: %s\n %s", err.ClassName(), err.Message(), stackTrace)
	switch common.ErrorCode(err.ErrorCode()) {
	case common.ErrorCodeAuthentication:
		return core.NewHazelcastAuthenticationError(message, nil)
	case common.ErrorCodeHazelcastInstanceNotActive:
		return core.NewHazelcastInstanceNotActiveError(message, nil)
	case common.ErrorCodeHazelcastSerialization:
		return core.NewHazelcastSerializationError(message, nil)
	case common.ErrorCodeTargetDisconnected:
		return core.NewHazelcastTargetDisconnectedError(message, nil)
	case common.ErrorCodeTargetNotMember:
		return core.NewHazelcastTargetNotMemberError(message, nil)
	case common.ErrorCodeUnsupportedOperation:
		return core.NewHazelcastUnsupportedOperationError(message, nil)
	case common.ErrorCodeConsistencyLostException:
		return core.NewHazelcastConsistencyLostError(message, nil)
	}

	return core.NewHazelcastErrorType(message, nil)
}
