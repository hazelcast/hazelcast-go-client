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
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/sirupsen/logrus"
)

func TestInvocation_UnwrapResponseWithUnknownType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Invocation should not unwrap an int type")
		}
	}()

	inv := invocation{}
	// should panic since invocation can only unwrap client message and error types
	inv.unwrapResponse(15)
}

func TestInvocation_ResultWithTimeout(t *testing.T) {
	inv := invocation{}
	now := time.Now()
	timeout := 1 * time.Second
	_, err := inv.ResultWithTimeout(timeout)
	passedTime := time.Since(now)
	if _, ok := err.(*core.HazelcastOperationTimeoutError); !ok {
		t.Errorf("Invocation should return a HazelcastOperationTimeoutError")
	}

	if passedTime > 2*time.Second {
		t.Errorf("ResultWithTimeout should timeout in %s", timeout)
	}

}

type dummy struct {
}

func TestInvocationService_Process(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Process should panic for unexpected type")
		}
	}()
	is := &invocationServiceImpl{
		responseChannel: make(chan interface{}, 1),
		logger:          logrus.New(),
	}
	go func() {
		// send dummy to make sure it wont fail in the future
		is.responseChannel <- dummy{}
	}()
	// TODO :: What if this does not panic? how will this method end?
	is.process()
}
