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

	"strconv"

	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
	"github.com/hazelcast/hazelcast-go-client/test/nearcache"
	"github.com/hazelcast/hazelcast-go-client/test/testutil"
	"github.com/stretchr/testify/assert"
)

func configureStalenessConfig(config *config.Config) {
	config.SetProperty(property.MaxToleratedMissCount.Name(), "0")
}

func TestNearCache_NotContainsStaleValue_WhenUpdatedByMultipleRoutines(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)

	nearCacheInvalidatorThreadCount := 1
	nearCachePutterThreadCount := 10
	mapName := "nearCachedMapDistortion"
	entryCount := 10
	var testRunning atomic.Value
	testRunning.Store(true)

	config := nearcache.CreateConfigWithDefaultNearCache()
	configureStalenessConfig(config)

	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	mp, _ := client.GetMap(mapName)
	for i := 0; i < nearCacheInvalidatorThreadCount; i++ {
		go func() {
			for testRunning.Load() == true {
				err := populateMapWithRandomValueFromServer(cluster.ID, mapName, entryCount)
				assert.NoError(t, err)
			}
		}()
	}

	for i := 0; i < nearCachePutterThreadCount; i++ {
		go func() {
			for testRunning.Load() == true {
				for i := 0; i < entryCount; i++ {
					mp.Get(int32(i))
				}
			}
		}()
	}
	time.Sleep(5 * time.Second)
	testRunning.Store(false)
	service := client.(*internal.HazelcastClient).SerializationService

	valuesFromMember := allValuesFromMember(t, cluster.ID, entryCount, mp.Name(), service)
	testutil.AssertTrueEventually(t, func() bool {
		return AssertNoStaleDataExistInNearCache(t, entryCount, mp, valuesFromMember)
	})
	rcMutex.Lock()
	remoteController.ShutdownCluster(cluster.ID)
	rcMutex.Unlock()

}

func populateMapWithRandomValueFromServer(clusterID, mapName string, mapSize int) error {
	script := "from com.hazelcast.util import RandomPicker\n" +
		"map=instance_0.getMap('" + mapName + "')\n" +
		"for i in xrange(0, " + strconv.Itoa(mapSize) + "):\n" +
		" map.put(i, RandomPicker.getInt( " + strconv.Itoa(mapSize) + "))"
	return runScript(clusterID, script)
}

func AssertNoStaleDataExistInNearCache(t *testing.T, entryCount int, mp core.Map, valuesFromMember []interface{}) bool {
	valuesFromCache := allValuesFromNearCache(entryCount, mp)
	for i := 0; i < entryCount; i++ {
		if valuesFromMember[i] != valuesFromCache[i] {
			return false
		}
	}
	return true
}

func allValuesFromMember(t *testing.T, clusterID string, entryCount int, mapName string,
	service spi.SerializationService) []interface{} {
	values := make([]interface{}, 0)
	for i := 0; i < entryCount; i++ {
		value, err := getValueFromMember(clusterID, mapName, i, service)
		assert.NoError(t, err)
		values = append(values, value)
	}
	return values
}

func allValuesFromNearCache(entryCount int, mp core.Map) []interface{} {
	values := make([]interface{}, 0)
	for i := 0; i < entryCount; i++ {
		value, _ := mp.Get(int32(i))
		values = append(values, value)
	}
	return values
}
