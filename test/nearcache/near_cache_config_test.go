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
	"strconv"
	"testing"

	"time"

	"github.com/hazelcast/hazelcast-go-client"
	config2 "github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/stretchr/testify/assert"
)

func TestObjectMemoryFormat(t *testing.T) {
	config := CreateConfigWithDefaultNearCache()
	config.NearCacheConfig().SetInMemoryFormat(config2.InMemoryFormatObject)
	client, err := hazelcast.NewClientWithConfig(config)
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		mp.Put(strconv.Itoa(i), int32(i))
		value, err := mp.Get(strconv.Itoa(i))
		assert.NoError(t, err)
		assert.Equal(t, value, int32(i))
		value, err = mp.Get(strconv.Itoa(i))
		assert.NoError(t, err)
		assert.Equal(t, value, int32(i))
	}

	client.Shutdown()
}

func TestNearCacheExpirationWithIdleKeys(t *testing.T) {
	config := CreateConfigWithDefaultNearCache()
	config.NearCacheConfig().SetMaxIdleDuration(1 * time.Second)
	client, err := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		mp.Put(strconv.Itoa(i), i)
		mp.Get(strconv.Itoa(i))
		mp.Get(strconv.Itoa(i))
	}

	cache := GetNearCacheFromMap(mp)
	test.AssertEventually(t, func() bool {
		return cache.Size() == 0
	})

}

func TestNearCacheEvictionPolicyNone(t *testing.T) {
	config := CreateConfigWithDefaultNearCache()
	config.NearCacheConfig().SetEvictionPolicy(config2.EvictionPolicyNone)
	config.NearCacheConfig().SetMaxEntryCount(10)
	client, err := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		mp.Put(strconv.Itoa(i), i)
		mp.Get(strconv.Itoa(i))
		mp.Get(strconv.Itoa(i))
	}

	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 10)
	for i := 0; i < 10; i++ {
		value := cache.Get(strconv.Itoa(i))
		assert.Equal(t, value, int64(i))
	}

}

func TestNearCacheMaxIdleDuration(t *testing.T) {
	config := CreateConfigWithDefaultNearCache()
	config.NearCacheConfig().SetMaxIdleDuration(100 * time.Millisecond)
	client, err := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		mp.Put(strconv.Itoa(i), i)
		mp.Get(strconv.Itoa(i))
		mp.Get(strconv.Itoa(i))
	}
	cache := GetNearCacheFromMap(mp)
	test.AssertAlwaysTrueFor(t, 5*time.Second, func() bool {
		preventFromBeingIdle("0", mp)
		value := cache.Get("0")
		return value != nil && value == int64(0)
	})

}
