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

package client

import (
	"sync"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/test/testutil"
	"github.com/stretchr/testify/assert"
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
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	config := hazelcast.NewConfig()
	lifecycleListener := lifecycleListener{wg: wg, collector: make([]string, 0)}
	config.AddLifecycleListener(&lifecycleListener)
	wg.Add(5)
	client, _ := hazelcast.NewClientWithConfig(config)
	client.Shutdown()
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "LifecycleService listener failed")
	assert.Equalf(t, lifecycleListener.collector[0], core.LifecycleStateStarting, "LifecycleService listener failed")
	assert.Equalf(t, lifecycleListener.collector[1], core.LifecycleStateConnected, "LifecycleService listener failed")
	assert.Equalf(t, lifecycleListener.collector[2], core.LifecycleStateStarted, "LifecycleService listener failed")
	assert.Equalf(t, lifecycleListener.collector[3], core.LifecycleStateShuttingDown, "LifecycleService listener failed")
	assert.Equalf(t, lifecycleListener.collector[4], core.LifecycleStateShutdown, "LifecycleService listener failed")
	remoteController.ShutdownCluster(cluster.ID)
}

func TestLifecycleListenerForDisconnected(t *testing.T) {
	cluster, _ = remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	var wg = new(sync.WaitGroup)
	lifecycleListener := lifecycleListener{wg: wg, collector: make([]string, 0)}
	wg.Add(1)
	config := hazelcast.NewConfig()
	config.NetworkConfig().SetConnectionAttemptLimit(100)
	client, _ := hazelcast.NewClientWithConfig(config)
	registrationID := client.LifecycleService().AddLifecycleListener(&lifecycleListener)
	remoteController.ShutdownCluster(cluster.ID)
	timeout := testutil.WaitTimeout(wg, testutil.Timeout)
	assert.Equalf(t, false, timeout, "LifecycleService listener failed")
	assert.Equalf(t, lifecycleListener.collector[0], core.LifecycleStateDisconnected, "LifecycleService listener failed")
	client.LifecycleService().RemoveLifecycleListener(registrationID)
	client.Shutdown()
}

func TestRemoveListener(t *testing.T) {
	shutdownFunc := testutil.CreateCluster(remoteController)
	defer shutdownFunc()

	var wg = new(sync.WaitGroup)
	lifecycleListener := lifecycleListener{wg: wg, collector: make([]string, 0)}
	client, _ := hazelcast.NewClient()
	registrationID := client.LifecycleService().AddLifecycleListener(&lifecycleListener)
	wg.Add(2)
	client.LifecycleService().RemoveLifecycleListener(registrationID)
	client.Shutdown()
	timeout := testutil.WaitTimeout(wg, testutil.TimeoutShort)
	assert.Equalf(t, true, timeout, "LifecycleService listener failed")
	assert.Equalf(t, len(lifecycleListener.collector), 0, "LifecycleService addListener or removeListener failed")
}
