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

package flakeidgen

import (
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var flakeIDGenerator core.FlakeIDGenerator
var client hazelcast.Client

const (
	flakeIDStep             = 1 << 16
	shortTermBatchSize      = 3
	shortTermValidityMillis = 3000
	numRoutines             = 4
	idsInRoutine            = 100000
)

var remoteController rc.RemoteController

func TestMain(m *testing.M) {
	var err error
	remoteController, err = rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	m.Run()
}

func asssignOverFlowID(clusterID string, instanceNum int) (r *rc.Response, err error) {
	script := "function assignOverflowedNodeId() {" +
		"   instance_" + strconv.Itoa(instanceNum) + ".getCluster().getLocalMember().setMemberListJoinVersion(100000);" +
		"   return instance_" + strconv.Itoa(instanceNum) + ".getCluster().getLocalMember().getMemberListJoinVersion();" +
		"}" +
		"result=\"\"+assignOverflowedNodeId();"
	return remoteController.ExecuteOnController(clusterID, script, 1)
}

func TestFlakeIDGeneratorProxy_ConfigPanicsWithInvalidPrefetchCount(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Negative prefetch count should panic.")
		}
	}()
	flakeIDCfg := config.NewFlakeIDGeneratorConfig("gen")
	flakeIDCfg.SetPrefetchCount(-1)

}

func TestFlakeIDGeneratorProxy_ConfigPanicsWithInvalidPrefetchCount2(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Negative prefetch count should panic.")
		}
	}()
	config.NewFlakeIDGeneratorConfigWithParameters("gen", -1, 10)
}

func TestFlakeIDGeneratorProxy_ConfigPanicsWithInvalidPrefetchValidityMillis(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Negative prefetchValidityMillis count should panic.")
		}
	}()
	flakeIDCfg := config.NewFlakeIDGeneratorConfig("gen")
	flakeIDCfg.SetPrefetchValidityMillis(-1)

}

func TestFlakeIDGeneratorProxy_ConfigPanicsWithInvalidPrefetchValidityMillis2(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Negative prefetchValidityMillis count should panic.")
		}
	}()
	config.NewFlakeIDGeneratorConfigWithParameters("gen", 10, -1)
}

func TestFlakeIDGeneratorProxy_ConfigTest(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	var myBatchSize int32 = shortTermBatchSize
	flakeIDConf := config.NewFlakeIDGeneratorConfig("gen")
	flakeIDConf.SetName("gen") // this is redundant, just to check if it doesnt mess anything.
	flakeIDConf.SetPrefetchCount(myBatchSize)
	flakeIDConf.SetPrefetchValidityMillis(shortTermValidityMillis)
	config := hazelcast.NewConfig()
	config.AddFlakeIDGeneratorConfig(flakeIDConf)
	client, _ = hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	flakeIDGenerator, _ = client.GetFlakeIDGenerator("gen")
	// this should take a batch of 3 IDs from the member and store it in the auto-batcher
	id1, err := flakeIDGenerator.NewID()
	require.NoError(t, err)
	// this should take second ID from auto-created batch. It should be exactly next to id1
	id2, err := flakeIDGenerator.NewID()
	require.NoError(t, err)
	assert.Equalf(t, id1+flakeIDStep, id2, "FlakeIDGenerator NewID() failed")

	time.Sleep(shortTermValidityMillis * time.Millisecond)
	// this ID should be from a new batch, because the validity elapsed
	id3, err := flakeIDGenerator.NewID()
	require.NoError(t, err)
	if id1+flakeIDStep*shortTermBatchSize >= id3 {
		assert.Fail(t, "FlakeIDGenerator NewID() failed")
	}

}

func TestFlakeIDGeneratorProxy_ConcurrentlyGeneratedIds(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	client, _ := hazelcast.NewClient()
	defer client.Shutdown()
	flakeIDGenerator, _ = client.GetFlakeIDGenerator("gen")
	localIdsSlice := make([]map[int64]struct{}, 0)
	ids := make(map[int64]struct{})
	wg.Add(numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func() {
			localIds := make(map[int64]struct{})
			for k := 0; k < idsInRoutine; k++ {
				curID, _ := flakeIDGenerator.NewID()
				localIds[curID] = struct{}{}
			}
			mu.Lock()
			localIdsSlice = append(localIdsSlice, localIds)
			mu.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	for _, localIds := range localIdsSlice {
		for id := range localIds {
			ids[id] = struct{}{}
		}
	}
	// if there were duplicate IDs generated, there will be less items in the map than expected
	assert.Equalf(t, len(ids), numRoutines*idsInRoutine, "FlakeIDGenerator NewID() returned duplicate ids")
}

func TestFlakeIDGeneratorProxy_WhenAllMembersOutOfRangeThenError(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	_, err := asssignOverFlowID(cluster.ID, 0)
	require.NoError(t, err)
	_, err = asssignOverFlowID(cluster.ID, 1)
	require.NoError(t, err)
	client, _ := hazelcast.NewClient()
	defer client.Shutdown()
	flakeIDGenerator, _ := client.GetFlakeIDGenerator("test")
	_, err = flakeIDGenerator.NewID()
	require.Errorf(t, err, "flakeIDGenerator should return an error when there is no server with a join id smaller than 2^16")
	if _, ok := err.(core.HazelcastError); !ok {
		t.Fatal("HazelcastError is expected when there is no server with a join id smaller than 2^16")
	}
}

func TestFlakeIDGeneratorProxy_WhenMemberOutOfRangeThenOtherMemberUsed(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)
	_, err := asssignOverFlowID(cluster.ID, 0)
	require.NoError(t, err)
	client, _ := hazelcast.NewClient()
	defer client.Shutdown()
	flakeIDGenerator, _ := client.GetFlakeIDGenerator("test")
	_, err = flakeIDGenerator.NewID()
	require.NoError(t, err)
}
