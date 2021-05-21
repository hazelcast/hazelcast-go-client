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

package invocation_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/stretchr/testify/assert"
)

func TestImpl_CanRetry(t *testing.T) {
	msg := proto.NewClientMessage(proto.NewFrame(make([]byte, 64)))
	inv := invocation.NewImpl(msg, 0, nil, time.Now().Add(10*time.Second), false)
	err := errors.New("foo")
	targetDisconnectedErr := hzerrors.NewHazelcastTargetDisconnectedError("foo", nil)
	noRetries := []error{err, cb.WrapNonRetryableError(err), targetDisconnectedErr}
	for _, e := range noRetries {
		if !assert.False(t, inv.CanRetry(e)) {
			t.FailNow()
		}
	}
	inv = invocation.NewImpl(msg, 0, nil, time.Now().Add(10*time.Second), true)
	if !assert.True(t, inv.CanRetry(targetDisconnectedErr)) {
		t.FailNow()
	}
	ioErr := hzerrors.NewHazelcastIOError("foo", nil)
	instNotActiveErr := hzerrors.NewHazelcastInstanceNotActiveError("foo", nil)
	yesRetries := []error{ioErr, instNotActiveErr}
	for _, e := range yesRetries {
		if !assert.True(t, inv.CanRetry(e)) {
			t.FailNow()
		}
	}
	msg = proto.NewClientMessage(proto.NewFrame(make([]byte, 64)))
	msg.SetRetryable(true)
	inv = invocation.NewImpl(msg, 0, nil, time.Now().Add(10*time.Second), false)
	if !assert.True(t, inv.CanRetry(targetDisconnectedErr)) {
		t.FailNow()
	}
}
