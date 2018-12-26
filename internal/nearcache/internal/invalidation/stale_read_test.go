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

package invalidation_test

import (
	"sync/atomic"
	"testing"

	"time"

	hazelcast "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/hazelcast/hazelcast-go-client/test/nearcache"
	"github.com/stretchr/testify/assert"
)

var key = "testKey"
var assertionViolationCount = int64(0)
var numGetters = 7
var maxRuntime = 15
var lastPutValue interface{}

func TestNoLostInvalidationsStrict(t *testing.T) {
	testNoLostInvalidations(t, true)
}

func TestNoLostInvalidationsEventually(t *testing.T) {
	testNoLostInvalidations(t, false)
}

func testNoLostInvalidations(t *testing.T, strict bool) {
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)

	mapName := "nearCachedMapDistortion"
	var testRunning = new(atomic.Value)
	testRunning.Store(true)

	config := nearcache.CreateConfigWithDefaultNearCache()
	configureStalenessConfig(config)

	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	mp, _ := client.GetMap(mapName)

	runTestInternal(testRunning, mp)

	if !strict {
		time.Sleep(10 * time.Second)
	}

	lastValue, _ := mp.Get(key)
	assert.Equal(t, lastPutValue, lastValue)

	if strict {
		assert.Zero(t, assertionViolationCount)
	}
	rcMutex.Lock()
	remoteController.ShutdownCluster(cluster.ID)
	rcMutex.Unlock()

}

func runTestInternal(testRunning *atomic.Value, mp core.Map) {
	go putInternal(testRunning, mp)
	time.Sleep(300 * time.Millisecond)

	for i := 0; i < numGetters; i++ {
		go getInternal(testRunning, mp)
	}

	runtime := 0
	for testRunning.Load() == true && runtime < maxRuntime {
		runtime++
		time.Sleep(1 * time.Second)
	}

	testRunning.Store(false)
}

func putInternal(testRunning *atomic.Value, mp core.Map) {
	i := int32(0)
	for testRunning.Load() == true {
		i++
		lastPutValue = i
		mp.Put(key, i)

		value, _ := mp.Get(key)
		if value == nil {
			continue
		}
		if value != i {
			atomic.AddInt64(&assertionViolationCount, 1)
			time.Sleep(100 * time.Millisecond)
			value, _ = mp.Get(key)
			if value != i {
				testRunning.Store(false)
			}
		}
	}
}

func getInternal(testRunning *atomic.Value, mp core.Map) {
	for testRunning.Load() == true {
		mp.Get(key)
	}
}
