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

package flake_id_generator

import (
	"github.com/hazelcast/hazelcast-go-client"
	. "github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/rc"
	. "github.com/hazelcast/hazelcast-go-client/tests"
	"log"
	"sync"
	"testing"
	"time"
)

var flakeIdGenerator core.FlakeIdGenerator
var client hazelcast.IHazelcastInstance

const (
	flakeIdStep             = 1 << 16
	shortTermBatchSize      = 3
	shortTermValidityMillis = 3000
	numRoutines             = 4
	idsInRoutine            = 100000
)

func TestMain(m *testing.M) {
	remoteController, err := NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, err := remoteController.CreateCluster("3.9", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	m.Run()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestFlakeIdGeneratorProxy_ConfigTest(t *testing.T) {
	var myBatchSize int32 = shortTermBatchSize
	config := hazelcast.NewHazelcastConfig().
		AddFlakeIdGeneratorConfig(NewFlakeIdGeneratorConfig("gen").SetPrefetchCount(myBatchSize).
			SetPrefetchValidityMillis(shortTermValidityMillis))
	client, _ = hazelcast.NewHazelcastClientWithConfig(config)
	defer client.Shutdown()
	flakeIdGenerator, _ = client.GetFlakeIdGenerator("gen")
	// this should take a batch of 3 IDs from the member and store it in the auto-batcher
	id1, err := flakeIdGenerator.NewId()
	AssertErrorNil(t, err)
	// this should take second ID from auto-created batch. It should be exactly next to id1
	id2, err := flakeIdGenerator.NewId()
	AssertEqualf(t, err, id1+flakeIdStep, id2, "FlakeIdGenerator NewId() failed")

	time.Sleep(shortTermValidityMillis * time.Millisecond)
	// this ID should be from a new batch, because the validity elapsed
	id3, err := flakeIdGenerator.NewId()
	AssertLessThanf(t, err, id1+flakeIdStep*shortTermBatchSize, id3, "FlakeIdGenerator NewId() failed")

}

func TestFlakeIdGeneratorProxy_ConcurrentlyGeneratedIds(t *testing.T) {
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}
	client, _ := hazelcast.NewHazelcastClient()
	defer client.Shutdown()
	flakeIdGenerator, _ = client.GetFlakeIdGenerator("gen")
	localIdsSlice := make([]map[int64]struct{}, 0)
	ids := make(map[int64]struct{}, 0)
	wg.Add(numRoutines)
	for i := 0; i < numRoutines; i++ {
		go func() {
			localIds := make(map[int64]struct{}, 0)
			for k := 0; k < idsInRoutine; k++ {
				curId, _ := flakeIdGenerator.NewId()
				localIds[curId] = struct{}{}
			}
			mu.Lock()
			localIdsSlice = append(localIdsSlice, localIds)
			mu.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	for _, localIds := range localIdsSlice {
		for id, _ := range localIds {
			ids[id] = struct{}{}
		}
	}
	// if there were duplicate IDs generated, there will be less items in the map than expected
	AssertEqualf(t, nil, len(ids), numRoutines*idsInRoutine, "FlakeIdGenerator NewId() returned duplicate ids")
}
