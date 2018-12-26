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
	"errors"
	"math/rand"
	"strconv"
	"testing"

	"sync/atomic"

	"math"

	"time"

	"encoding/binary"

	"sync"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/internal/murmur"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/spi"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/hazelcast/hazelcast-go-client/test/nearcache"
	"github.com/stretchr/testify/assert"
)

func configureDistortionConfig(config *config.Config) {
	config.SetProperty(property.MaxToleratedMissCount.Name(), "0")
	config.SetProperty(property.ReconciliationIntervalSeconds.Name(), "10")
	config.SetProperty(property.MinReconciliationIntervalSeconds.Name(), "10")
	config.NearCacheConfig().SetMaxEntryCount(math.MaxInt32)
}

var expectedKeyValues = make(map[int]int)
var rcMutex = new(sync.Mutex)

func TestInvalidationDistortionSequenceAndUUID(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)

	mapName := "nearCachedMapDistortion"
	mapSize := 100000
	var testRunning atomic.Value
	testRunning.Store(true)

	config := nearcache.CreateConfigWithDefaultNearCache()
	configureDistortionConfig(config)

	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	mp, _ := client.GetMap(mapName)
	err := populateMapFromServer(cluster.ID, mapName, mapSize)
	assert.NoError(t, err)
	var rcMutex sync.Mutex
	populateNearCache := func() {
		for testRunning.Load() == true {
			for i := 0; i < mapSize; i++ {
				mp.Get(i)
			}
		}
	}

	distortSequence := func() {
		for testRunning.Load() == true {
			rcMutex.Lock()
			err := distortRandomPartitionSequence(cluster.ID, mapName)
			rcMutex.Unlock()
			assert.NoError(t, err)
			time.Sleep(1 * time.Second)
		}

	}

	distortUUID := func() {
		for testRunning.Load() == true {
			rcMutex.Lock()
			err := distortRandomPartitionUuid(cluster.ID)
			rcMutex.Unlock()
			assert.NoError(t, err)
			time.Sleep(5 * time.Second)
		}

	}

	putOnMember := func() {
		for testRunning.Load() == true {
			key := rand.Int31n(int32(mapSize))
			value := rand.Int31n(math.MaxInt32)
			rcMutex.Lock()
			err := putFromMember(cluster.ID, mapName, int(key), int(value))
			rcMutex.Unlock()
			assert.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
		}

	}

	go populateNearCache()
	go distortSequence()
	go distortUUID()
	go putOnMember()
	time.Sleep(30 * time.Second)
	testRunning.Store(false)

	test.AssertEventually(t, func() bool {
		for i := 0; i < mapSize; i++ {
			actualValue, _ := mp.Get(int32(i))
			value := expectedKeyValues[i]
			if actualValue != int32(value) {
				return false
			}
		}
		return true
	})
	rcMutex.Lock()
	remoteController.ShutdownCluster(cluster.ID)
	rcMutex.Unlock()

}

func populateMapFromServer(clusterID string, mapName string, mapSize int) error {
	for i := 0; i < mapSize; i++ {
		expectedKeyValues[i] = i
	}
	script := "map=instance_0.getMap('" + mapName + "')\nfor i in xrange(0, " + strconv.Itoa(mapSize) + "):\n" +
		" map.put(i,i)"
	return runScript(clusterID, script)
}

func distortRandomPartitionSequence(clusterID, mapName string) error {
	script := "from com.hazelcast.core import HazelcastInstance\n" +
		"from com.hazelcast.internal.nearcache.impl.invalidation import Invalidator\n" +
		"from com.hazelcast.internal.nearcache.impl.invalidation import MetaDataGenerator\n" +
		"from com.hazelcast.map.impl import MapService\n" +
		"from com.hazelcast.map.impl import MapServiceContext\n" +
		"from com.hazelcast.map.impl.nearcache import MapNearCacheManager\n" +
		"from com.hazelcast.spi.impl import NodeEngineImpl\n" +
		"from com.hazelcast.instance import TestUtil\n" +
		"from com.hazelcast.util import RandomPicker\n" +
		"from com.hazelcast.internal.partition import InternalPartitionService\n" +
		"from java.lang import Integer\n" +
		"nodeEngineImpl = TestUtil.getNode(instance_0).nodeEngine\n" +
		"mapService = nodeEngineImpl.getService(MapService.SERVICE_NAME)\n" +
		"mapServiceContext = mapService.getMapServiceContext()\n" +
		"mapNearCacheManager = mapServiceContext.getMapNearCacheManager()\n" +
		"invalidator = mapNearCacheManager.getInvalidator()\n" +
		"metaDataGenerator = invalidator.getMetaDataGenerator()\n" +
		"partitionService = nodeEngineImpl.getPartitionService()\n" +
		"partitionCount = partitionService.getPartitionCount()\n" +
		"metaDataGenerator.setCurrentSequence('" + mapName + "', RandomPicker.getInt(partitionCount), " +
		"RandomPicker.getInt(Integer.MAX_VALUE))"
	return runScript(clusterID, script)
}

func distortRandomPartitionUuid(clusterID string) error {
	script := "from com.hazelcast.core import HazelcastInstance\n" +
		"from com.hazelcast.internal.nearcache.impl.invalidation import Invalidator\n" +
		"from com.hazelcast.internal.nearcache.impl.invalidation import MetaDataGenerator\n" +
		"from com.hazelcast.map.impl import MapService\n" +
		"from com.hazelcast.map.impl import MapServiceContext\n" +
		"from com.hazelcast.map.impl.nearcache import MapNearCacheManager\n" +
		"from com.hazelcast.spi.impl import NodeEngineImpl\n" +
		"from com.hazelcast.instance import TestUtil\n" +
		"from com.hazelcast.util import RandomPicker\n" +
		"from com.hazelcast.util import UuidUtil\n" +
		"nodeEngineImpl = TestUtil.getNode(instance_0).nodeEngine\n" +
		"partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount()\n" +
		"partitionId = RandomPicker.getInt(partitionCount)\n" +
		"mapService = nodeEngineImpl.getService(MapService.SERVICE_NAME)\n" +
		"mapServiceContext = mapService.getMapServiceContext()\n" +
		"mapNearCacheManager = mapServiceContext.getMapNearCacheManager()\n" +
		"invalidator = mapNearCacheManager.getInvalidator()\n" +
		"metaDataGenerator = invalidator.getMetaDataGenerator()\n" +
		"metaDataGenerator.setUuid(partitionId, UuidUtil.newUnsecureUUID())"
	return runScript(clusterID, script)
}

func putFromMember(clusterID string, mapName string, key int, value int) error {
	expectedKeyValues[key] = value
	script := "map = instance_0.getMap('" + mapName + "');map.put(" + strconv.Itoa(key) + "," + strconv.Itoa(value) + ")"
	return runScript(clusterID, script)
}

func getValueFromMember(clusterID, mapName string, key int, service spi.SerializationService) (int32, error) {
	script := "from java.util import ArrayList\n" +
		"map=instance_0.getMap('" + mapName + "')\n" +
		"result=instance_0.getSerializationService().toBytes(map.get(" + strconv.Itoa(key) + "))"
	rcMutex.Lock()
	resp, err := remoteController.ExecuteOnController(clusterID, script, rc.Lang_PYTHON)
	rcMutex.Unlock()
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return 0, errors.New(resp.String())
	}
	data := NewData(resp.GetResult_())
	value, err := service.ToObject(data)
	return value.(int32), err

}

func runScript(clusterID, script string) error {
	rcMutex.Lock()
	resp, err := remoteController.ExecuteOnController(clusterID, script, rc.Lang_PYTHON)
	rcMutex.Unlock()
	if err != nil {
		return err
	}
	if !resp.Success {
		return errors.New(resp.String())
	}
	return nil
}

const (
	typeOffset       = 4
	DataOffset       = 8
	heapDataOverhead = 8
)

type Data struct {
	Payload []byte
}

// NewData return serialization Data with the given payload.
func NewData(payload []byte) serialization.Data {
	return &Data{payload}
}

func (d *Data) Buffer() []byte {
	return d.Payload
}

func (d *Data) GetType() int32 {
	if d.TotalSize() == 0 {
		return 0
	}
	return int32(binary.BigEndian.Uint32(d.Payload[typeOffset:]))
}

func (d *Data) TotalSize() int {
	if d.Payload == nil {
		return 0
	}
	return len(d.Payload)
}

func (d *Data) DataSize() int {
	return int(math.Max(float64(d.TotalSize()-heapDataOverhead), 0))
}

func (d *Data) GetPartitionHash() int32 {
	return murmur.Default3A(d.Payload, DataOffset, d.DataSize())
}
