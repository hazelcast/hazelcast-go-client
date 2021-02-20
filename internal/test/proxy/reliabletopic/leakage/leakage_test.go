// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package leakage

import (
	"log"
	"testing"

	"runtime"

	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/rc"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/reliabletopic"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/test/testutil"
	"github.com/stretchr/testify/assert"
)

var remoteController *rc.RemoteControllerClient

func TestMain(m *testing.M) {
	rc, err := rc.NewRemoteControllerClient("localhost:9701")
	remoteController = rc
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	m.Run()
}

func TestReliableTopicProxy_Leakage(t *testing.T) {
	shutdownFunc := testutil.CreateCluster(remoteController)
	defer shutdownFunc()
	routineNumBefore := runtime.NumGoroutine()
	client, _ := hazelcast.NewClient()
	reliableTopic, _ := client.GetReliableTopic("name")
	topic := reliableTopic.(*internal.ReliableTopicProxy)
	items := generateItems(client.(*internal.HazelcastClient), 20)
	_, err := topic.Ringbuffer().AddAll(items, core.OverflowPolicyOverwrite)
	assert.NoError(t, err)
	client.Shutdown()
	testutil.AssertTrueEventually(t, func() bool {
		routineNumAfter := runtime.NumGoroutine()
		return routineNumBefore == routineNumAfter
	})
}

func generateItems(client *internal.HazelcastClient, n int) []interface{} {
	items := make([]interface{}, n)
	for i := 1; i <= n; i++ {
		data, _ := client.SerializationService.ToData(i)
		items[i-1] = reliabletopic.NewMessage(data, nil)
	}
	return items
}
