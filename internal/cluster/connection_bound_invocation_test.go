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

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	icluster "github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/stretchr/testify/assert"
)

func TestConnectionBoundInvocation_CanRetry(t *testing.T) {
	fac := icluster.NewConnectionInvocationFactory(&pubcluster.Config{})
	msg := proto.NewClientMessage(proto.NewFrame(make([]byte, 64)))
	inv := fac.NewConnectionBoundInvocation(msg, nil, nil)
	err := errors.New("foo")
	if !assert.False(t, inv.CanRetry(err)) {
		t.FailNow()
	}
	nonretryableErr := cb.WrapNonRetryableError(err)
	if !assert.False(t, inv.CanRetry(nonretryableErr)) {
		t.FailNow()
	}
	ioErr := hzerrors.NewHazelcastIOError("foo", nil)
	if !assert.True(t, inv.CanRetry(ioErr)) {
		t.FailNow()
	}
	targetDisconnectedErr := hzerrors.NewHazelcastTargetDisconnectedError("foo", nil)
	if !assert.True(t, inv.CanRetry(targetDisconnectedErr)) {
		t.FailNow()
	}
}
