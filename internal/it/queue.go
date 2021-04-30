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

package it

import (
	"fmt"
	"math/rand"
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client"
	"go.uber.org/goleak"
)

func QueueTester(t *testing.T, f func(t *testing.T, q *hz.Queue)) {
	makeQueueName := func() string {
		return fmt.Sprintf("test-queue-%d-%d", idGen.NextID(), rand.Int())
	}
	QueueTesterWithConfigBuilderWithName(t, makeQueueName, nil, f)
}

func QueueTesterWithConfigBuilderWithName(t *testing.T, queueName func() string, cbCallback func(cb *hz.ConfigBuilder), f func(t *testing.T, q *hz.Queue)) {
	var (
		client *hz.Client
		q      *hz.Queue
	)
	ensureRemoteController(true)
	runner := func(t *testing.T, smart bool) {
		if LeakCheckEnabled() {
			t.Logf("enabled leak check")
			defer goleak.VerifyNone(t)
		}
		cb := defaultTestCluster.configBuilder()
		if cbCallback != nil {
			cbCallback(cb)
		}
		cb.Cluster().SetSmartRouting(smart)
		client, q = getClientQueueWithConfig(queueName(), cb)
		defer func() {
			if err := client.Shutdown(); err != nil {
				t.Logf("test warning, client not shutdown: %s", err.Error())
			}
		}()
		f(t, q)
	}
	if SmartEnabled() {
		t.Run("Smart Client", func(t *testing.T) {
			runner(t, true)
		})
	}
	if NonSmartEnabled() {
		t.Run("Non-Smart Client", func(t *testing.T) {
			runner(t, false)
		})
	}
}
