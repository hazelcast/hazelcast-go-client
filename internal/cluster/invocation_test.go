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

package cluster_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	icluster "github.com/hazelcast/hazelcast-go-client/internal/cluster"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestConnectionBoundInvocation_CanRetry(t *testing.T) {
	fac := icluster.NewConnectionInvocationFactory(&pubcluster.Config{})
	msg := proto.NewClientMessage(proto.NewFrame(make([]byte, 64)))
	inv := fac.NewConnectionBoundInvocation(msg, nil, nil, time.Now())
	err := errors.New("foo")
	if !assert.False(t, inv.CanRetry(err)) {
		t.FailNow()
	}
	nonretryableErr := cb.WrapNonRetryableError(err)
	if !assert.False(t, inv.CanRetry(nonretryableErr)) {
		t.FailNow()
	}
	ioErr := ihzerrors.NewIOError("foo", nil)
	if !assert.False(t, inv.CanRetry(ioErr)) {
		t.FailNow()
	}
	targetDisconnectedErr := ihzerrors.NewTargetDisconnectedError("foo", nil)
	if !assert.False(t, inv.CanRetry(targetDisconnectedErr)) {
		t.FailNow()
	}
}

func TestMemberBoundInvocation_CanRetry(t *testing.T) {
	msg := proto.NewClientMessage(proto.NewFrame(make([]byte, 64)))
	mi := &pubcluster.MemberInfo{Address: "", UUID: types.NewUUID()}
	inv := icluster.NewMemberBoundInvocation(msg, mi, time.Now().Add(10*time.Second), false)
	err := errors.New("foo")
	targetDisconnectedErr := ihzerrors.NewTargetDisconnectedError("foo", nil)
	targetNotMemberErr := ihzerrors.NewClientError("target not member", nil, hzerrors.ErrTargetNotMember)
	noRetries := []error{err, cb.WrapNonRetryableError(err), targetDisconnectedErr, targetNotMemberErr}
	for _, e := range noRetries {
		if !assert.False(t, inv.CanRetry(e)) {
			t.FailNow()
		}
	}
	inv = icluster.NewMemberBoundInvocation(msg, mi, time.Now().Add(10*time.Second), true)
	if !assert.True(t, inv.CanRetry(targetDisconnectedErr)) {
		t.FailNow()
	}
	ioErr := ihzerrors.NewIOError("foo", nil)
	instNotActiveErr := ihzerrors.NewInstanceNotActiveError("foo")
	yesRetries := []error{ioErr, instNotActiveErr}
	for _, e := range yesRetries {
		if !assert.True(t, inv.CanRetry(e)) {
			t.FailNow()
		}
	}
	msg = proto.NewClientMessage(proto.NewFrame(make([]byte, 64)))
	msg.SetRetryable(true)
	inv = icluster.NewMemberBoundInvocation(msg, mi, time.Now().Add(10*time.Second), false)
	if !assert.True(t, inv.CanRetry(targetDisconnectedErr)) {
		t.FailNow()
	}
}
