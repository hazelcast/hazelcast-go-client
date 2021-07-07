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
	"context"
	"fmt"
	"math/rand"
	"testing"

	"go.uber.org/goleak"

	hz "github.com/hazelcast/hazelcast-go-client"
)

func QueueTester(t *testing.T, f func(t *testing.T, q *hz.Queue)) {
	makeQueueName := func() string {
		return fmt.Sprintf("test-queue-%d-%d", idGen.NextID(), rand.Int())
	}
	QueueTesterWithConfigAndName(t, makeQueueName, nil, f)
}

func QueueTesterWithConfigAndName(t *testing.T, queueName func() string, configCallback func(*hz.Config), f func(t *testing.T, q *hz.Queue)) {
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
		config := defaultTestCluster.DefaultConfig()
		if configCallback != nil {
			configCallback(&config)
		}
		config.Cluster.Unisocket = !smart
		client, q = getClientQueueWithConfig(queueName(), &config)
		defer func() {
			if err := q.Destroy(context.Background()); err != nil {
				t.Logf("test warning, could not destroy queue: %s", err.Error())
			}
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

func getClientQueueWithConfig(name string, config *hz.Config) (*hz.Client, *hz.Queue) {
	client := getDefaultClient(config)
	if q, err := client.GetQueue(context.Background(), name); err != nil {
		panic(err)
	} else {
		return client, q
	}
}
