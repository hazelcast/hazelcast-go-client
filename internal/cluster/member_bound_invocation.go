/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cluster

import (
	"errors"
	"time"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type MemberBoundInvocation struct {
	*invocation.Impl
	memberUUID types.UUID
}

func NewMemberBoundInvocation(msg *proto.ClientMessage, member *pubcluster.MemberInfo, deadline time.Time, redoOperation bool) *MemberBoundInvocation {
	inv := invocation.NewImplWithMemberUUID(msg, member.UUID, deadline, redoOperation)
	return &MemberBoundInvocation{Impl: inv, memberUUID: member.UUID}
}

func (i *MemberBoundInvocation) CanRetry(err error) bool {
	var nonRetryableError *cb.NonRetryableError
	if errors.As(err, &nonRetryableError) {
		return false
	}
	if errors.Is(err, hzerrors.ErrTargetNotMember) {
		return false
	}
	return i.MaybeCanRetry(err)
}
