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
	"runtime"
	"testing"

	"strconv"

	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/stretchr/testify/assert"
)

func TestNearCacheLeakageWhenDefault(t *testing.T) {
	routineNumBefore := runtime.NumGoroutine()
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		mp.Put(i, i)
		mp.Get(i)
		mp.Get(i)
	}

	client.Shutdown()
	test.AssertEventually(t, func() bool {
		routineNumAfter := runtime.NumGoroutine()
		return routineNumAfter == routineNumBefore
	})
}

func TestNearCacheLeakageWhenEvictionDone(t *testing.T) {
	routineNumBefore := runtime.NumGoroutine()
	config := CreateConfigWithDefaultNearCache()
	config.NearCacheConfig().SetMaxEntryCount(10)
	client, err := hazelcast.NewClientWithConfig(config)
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		mp.Put(strconv.Itoa(i), i)
		mp.Get(strconv.Itoa(i))
		mp.Get(strconv.Itoa(i))
	}

	client.Shutdown()
	test.AssertEventually(t, func() bool {
		routineNumAfter := runtime.NumGoroutine()
		return routineNumAfter == routineNumBefore
	})
}

func TestNearCacheLeakageWhenExpirationDone(t *testing.T) {
	routineNumBefore := runtime.NumGoroutine()
	config := CreateConfigWithDefaultNearCache()
	config.NearCacheConfig().SetTimeToLive(1 * time.Second)
	client, err := hazelcast.NewClientWithConfig(config)
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		mp.Put(strconv.Itoa(i), i)
		mp.Get(strconv.Itoa(i))
		mp.Get(strconv.Itoa(i))
	}

	client.Shutdown()
	test.AssertEventually(t, func() bool {
		routineNumAfter := runtime.NumGoroutine()
		return routineNumAfter == routineNumBefore
	})
}
