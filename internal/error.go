// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/common"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

func CreateHazelcastError(err *protocol.Error) core.HazelcastError {
	switch ErrorCode(err.ErrorCode()) {
	case ERROR_CODE_AUTHENTICATION:
		return core.NewHazelcastAuthenticationError(err.Message(), nil)
	case ERROR_CODE_HAZELCAST_INSTANCE_NOT_ACTIVE:
		return core.NewHazelcastInstanceNotActiveError(err.Message(), nil)
	case ERROR_CODE_HAZELCAST_SERIALIZATION:
		return core.NewHazelcastSerializationError(err.Message(), nil)
	case ERROR_CODE_TARGET_DISCONNECTED:
		return core.NewHazelcastTargetDisconnectedError(err.Message(), nil)
	case ERROR_CODE_TARGET_NOT_MEMBER:
		return core.NewHazelcastTargetNotMemberError(err.Message(), nil)
	}
	return core.NewHazelcastErrorType(err.Message(), nil)
}
