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

package nearcache

import (
	"log"
	"testing"

	"time"

	"strconv"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	remoteController, err := rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	m.Run()
	remoteController.ShutdownCluster(cluster.ID)
}

func TestNearCache(t *testing.T) {
	nearCacheCfg := config.NewNearCacheConfig()
	config := hazelcast.NewConfig()
	config.SetNearCacheConfig(nearCacheCfg)
	client, err := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	limit := 1
	for i := 0; i < limit; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		_, err = mp.Put(key, value)
		assert.NoError(t, err)

		now := time.Now()
		actualValue, err := mp.Get(key)
		remoteGetTime := time.Since(now)
		assert.NoError(t, err)
		assert.Equal(t, actualValue, value)

		now = time.Now()
		actualValue, err = mp.Get(key)
		cacheGetTime := time.Since(now)
		assert.NoError(t, err)
		assert.Equal(t, actualValue, value)

		if remoteGetTime < cacheGetTime {
			t.Error("remote operation takes less time than reading from cache", remoteGetTime, cacheGetTime)
		}
	}
}
