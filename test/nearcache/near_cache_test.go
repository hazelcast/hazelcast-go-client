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
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/predicate"
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

func TestNearCacheEachGetFasterFromCache(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	limit := 1000
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

func TestNearCacheContainsKey(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	mp.Get("invalidKey")

	found, err := mp.ContainsKey("key")
	assert.NoError(t, err)
	assert.True(t, found)

	found, err = mp.ContainsKey("invalidKey")
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestNearCacheGet(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	value := cache.Get("key")
	assert.Equal(t, value, "value")
}

func TestNearCacheGetAll(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	size := 100
	keys := make([]interface{}, 0)
	for i := 0; i < size; i++ {
		mp.Put("key"+strconv.Itoa(i), "value"+strconv.Itoa(i))
		keys = append(keys, "key"+strconv.Itoa(i))
	}

	mp.GetAll(keys)
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 100)
}

func TestNearCacheIdleKeysExpire(t *testing.T) {
	config := CreateConfigWithDefaultNearCache()
	config.NearCacheConfig().SetMaxIdleDuration(2 * time.Second)

	client, err := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	keys := make([]interface{}, 0)
	for i := int32(0); i < 10; i++ {
		keys = append(keys, i)
		mp.Put(keys[i], keys[i])
	}

	nonIdleKey := int64(100)
	mp.Put(nonIdleKey, nonIdleKey)

	mp.GetAll(keys)
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), len(keys))

	test.AssertEventually(t, func() bool {
		preventFromBeingIdle(nonIdleKey, mp)
		cacheSize := cache.Size()
		if cacheSize != 1 {
			return false
		}
		nonIdleValue := cache.Get(nonIdleKey)
		return nonIdleValue == nonIdleKey
	})
}

func TestInvalidateOnPut(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.Put("key", "value2")
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnRemove(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.Remove("key")
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnDelete(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.Delete("key")
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnEvict(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.Evict("key")
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnRemoveIfSame(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.RemoveIfSame("key", "value")
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnTryRemove(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.TryRemove("key", 10*time.Second)
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnTryPut(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.TryPut("key", "value2")
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnTryPutWithTimeout(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.TryPutWithTimeout("key", "value2", 10*time.Second)
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnRemoveAll(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Put("key2", "value")
	mp.Get("key")
	mp.Get("key2")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 2)

	mp.RemoveAll(predicate.NewEqual("this", "key"))
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnClear(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Put("key2", "value")
	mp.Get("key")
	mp.Get("key2")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 2)

	mp.Clear()
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnEvictAll(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Put("key2", "value")
	mp.Get("key")
	mp.Get("key2")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 2)

	mp.Clear()
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnReplace(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.Replace("key", "value2")
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnReplaceIfSame(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.ReplaceIfSame("key", "value2", "value")
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnSet(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.Set("key", "value2")
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnSetWithTTL(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.SetWithTTL("key", "value2", 10*time.Second)
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnPutAll(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Put("key2", "value")
	mp.Get("key")
	mp.Get("key2")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 2)
	entries := make(map[interface{}]interface{})
	entries["key"] = "value3"
	mp.PutAll(entries)
	assert.Equal(t, cache.Size(), 1)
}

func TestInvalidateOnPutTransient(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.PutTransient("key", "value2", 10*time.Second)
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnExecuteOnKey(t *testing.T) {
	client, err := hazelcast.NewClientWithConfig(CreateConfigWithDefaultNearCache())
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.ExecuteOnKey("key", nil)
	assert.Equal(t, cache.Size(), 0)
}

func preventFromBeingIdle(key interface{}, mp core.Map) {
	mp.Get(key)
}
