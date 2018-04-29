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

package tests

import (
	"sync"
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/tests/assert"
)

type lifecycleListener struct {
	wg        *sync.WaitGroup
	collector []string
}

func (l *lifecycleListener) LifecycleStateChanged(newState string) {
	l.collector = append(l.collector, newState)
	l.wg.Done()
}

func TestLifecycleListener(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	config := hazelcast.NewHazelcastConfig()
	lifecycleListener := lifecycleListener{wg: wg, collector: make([]string, 0)}
	config.AddLifecycleListener(&lifecycleListener)
	remoteController.StartMember(cluster.ID)
	wg.Add(5)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	client.Shutdown()
	timeout := WaitTimeout(wg, Timeout)
	assert.Equalf(t, nil, false, timeout, "Lifecycle listener failed")
	assert.Equalf(t, nil, lifecycleListener.collector[0], internal.LifecycleStateStarting, "Lifecycle listener failed")
	assert.Equalf(t, nil, lifecycleListener.collector[1], internal.LifecycleStateConnected, "Lifecycle listener failed")
	assert.Equalf(t, nil, lifecycleListener.collector[2], internal.LifecycleStateStarted, "Lifecycle listener failed")
	assert.Equalf(t, nil, lifecycleListener.collector[3], internal.LifecycleStateShuttingDown, "Lifecycle listener failed")
	assert.Equalf(t, nil, lifecycleListener.collector[4], internal.LifecycleStateShutdown, "Lifecycle listener failed")
	remoteController.ShutdownCluster(cluster.ID)
}

func TestLifecycleListenerForDisconnected(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	lifecycleListener := lifecycleListener{wg: wg, collector: make([]string, 0)}
	remoteController.StartMember(cluster.ID)
	wg.Add(1)
	config := hazelcast.NewHazelcastConfig()
	config.ClientNetworkConfig().SetConnectionAttemptLimit(100)
	client, _ := hazelcast.NewHazelcastClientWithConfig(config)
	registrationID := client.(*internal.HazelcastClient).LifecycleService.AddListener(&lifecycleListener)
	remoteController.ShutdownCluster(cluster.ID)
	timeout := WaitTimeout(wg, Timeout)
	assert.Equalf(t, nil, false, timeout, "Lifecycle listener failed")
	assert.Equalf(t, nil, lifecycleListener.collector[0], internal.LifecycleStateDisconnected, "Lifecycle listener failed")
	client.Lifecycle().RemoveListener(&registrationID)
	client.Shutdown()
}

func TestRemoveListener(t *testing.T) {
	var wg = new(sync.WaitGroup)
	cluster, _ = remoteController.CreateCluster("", DefaultServerConfig)
	lifecycleListener := lifecycleListener{wg: wg, collector: make([]string, 0)}
	remoteController.StartMember(cluster.ID)
	client, _ := hazelcast.NewHazelcastClient()
	registrationID := client.Lifecycle().AddListener(&lifecycleListener)
	wg.Add(2)
	client.Lifecycle().RemoveListener(&registrationID)
	client.Shutdown()
	timeout := WaitTimeout(wg, Timeout/20)
	assert.Equalf(t, nil, true, timeout, "Lifecycle listener failed")
	assert.Equalf(t, nil, len(lifecycleListener.collector), 0, "Lifecycle addListener or removeListener failed")
	remoteController.ShutdownCluster(cluster.ID)
}
