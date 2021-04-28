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

package hazelcast_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestRetryWithoutRedoOperation(t *testing.T) {
	// The connection should be retried on IOError
	retryResult(t, false, false)
}

func retryResult(t *testing.T, redo bool, target bool) {
	cluster := it.StartNewCluster(1)
	addr := fmt.Sprintf("localhost:%d", it.DefaultPort)
	cb := hz.NewConfigBuilder()
	cb.Cluster().
		SetName(it.DefaultClusterName).
		SetAddrs(addr).
		SetRedoOperation(redo)
	client := it.MustClient(hz.StartNewClientWithConfig(cb))
	m := it.MustValue(client.GetMap("redo-test")).(*hz.Map)
	// ensure that operations complete OK.
	//it.Must(m.Set("key", "value"))
	// shutdown the cluster and try again
	cluster.Shutdown()
	okCh := make(chan bool)
	go func(ch chan<- bool) {
		// set is a non-retryable operation
		if err := m.Set("key", "value"); err != nil {
			ch <- false
		} else {
			ch <- true
		}
	}(okCh)
	time.Sleep(1 * time.Second)
	cluster = it.StartNewCluster(1)
	defer cluster.Shutdown()
	time.Sleep(5 * time.Second)
	ok := <-okCh
	assert.Equal(t, target, ok)
}
