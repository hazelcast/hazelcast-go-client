/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package hazelcast_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestRetryWithoutRedoOperation(t *testing.T) {
	// The connection should be retried on IOError
	retryResult(t, false, true)
}

func TestProxy_Destroy(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		if err := m.Destroy(context.Background()); err != nil {
			t.Fatal(err)
		}
		// the next call should do nothing and return no error
		if err := m.Destroy(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func retryResult(t *testing.T, redo bool, target bool) {
	cluster := it.StartNewClusterWithOptions("TestProxy_Destroy", 15701, 1)
	config := cluster.DefaultConfig()
	config.Cluster.RedoOperation = redo
	ctx := context.Background()
	client := it.MustClient(hz.StartNewClientWithConfig(ctx, config))
	m := it.MustValue(client.GetMap(ctx, "redo-test")).(*hz.Map)
	// shutdown the cluster and try again
	cluster.Shutdown()
	okCh := make(chan bool)
	go func(ch chan<- bool) {
		// set is a non-retryable operation
		if err := m.Set(context.Background(), "key", "value"); err != nil {
			ch <- false
		} else {
			ch <- true
		}
	}(okCh)
	time.Sleep(1 * time.Second)
	cluster = it.StartNewClusterWithOptions("TestProxy_Destroy", 15701, 1)
	defer cluster.Shutdown()
	ok := <-okCh
	assert.Equal(t, target, ok)
}
