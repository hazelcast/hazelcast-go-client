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
	for _, cfg := range CreateNearCacheConfigs() {
		testNearCacheEachGetFasterFromCache(t, cfg)
	}
}

func testNearCacheEachGetFasterFromCache(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	limit := 1000
	getRemoteTotal := int64(0)
	getCacheTotal := int64(0)

	bucketSize := 10
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
		getRemoteTotal += remoteGetTime.Nanoseconds()
		getCacheTotal += cacheGetTime.Nanoseconds()
		if i%bucketSize == 0 {
			if getRemoteTotal < getCacheTotal {
				t.Error("remote operation takes less time than reading from cache", getRemoteTotal, getCacheTotal)
			}
			getRemoteTotal = 0
			getCacheTotal = 0
		}
	}

}

func TestNearCacheContainsKey(t *testing.T) {
	for _, cfg := range CreateNearCacheConfigs() {
		testNearCacheContainsKey(t, cfg)
	}
}

func testNearCacheContainsKey(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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

func TestNearCacheGetDataKey(t *testing.T) {
	config := CreateConfigWithDefaultNearCache()
	config.NearCacheConfig().SetSerializeKeys(true)
	testNearCacheGet(t, config)
}

func TestNearCacheGetObjectKey(t *testing.T) {
	config := CreateConfigWithDefaultNearCache()
	config.NearCacheConfig().SetSerializeKeys(false)
	testNearCacheGet(t, config)
}

func TestNearCacheGetDataValue(t *testing.T) {
	cfg := CreateConfigWithDefaultNearCache()
	cfg.NearCacheConfig().SetInMemoryFormat(config.InMemoryFormatBinary)
	testNearCacheGet(t, cfg)
}

func TestNearCacheGetObjectValue(t *testing.T) {
	cfg := CreateConfigWithDefaultNearCache()
	cfg.NearCacheConfig().SetInMemoryFormat(config.InMemoryFormatObject)
	testNearCacheGet(t, cfg)
}

func testNearCacheGet(t *testing.T, config *config.Config) {
	client, err := hazelcast.NewClientWithConfig(config)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testNearCacheGetAll(t, cfg)
	}
}

func testNearCacheGetAll(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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

	for i := 0; i < size; i++ {
		value := cache.Get("key" + strconv.Itoa(i))
		assert.Equal(t, value, "value"+strconv.Itoa(i))
	}
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnPut(t, cfg)
	}
}
func testInvalidateOnPut(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnRemove(t, cfg)
	}
}

func testInvalidateOnRemove(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnDelete(t, cfg)
	}
}

func testInvalidateOnDelete(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnEvict(t, cfg)
	}
}

func testInvalidateOnEvict(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnRemoveIfSame(t, cfg)
	}
}

func testInvalidateOnRemoveIfSame(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnTryRemove(t, cfg)
	}
}

func testInvalidateOnTryRemove(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnTryPut(t, cfg)
	}
}

func testInvalidateOnTryPut(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnTryPutWithTimeout(t, cfg)
	}
}

func testInvalidateOnTryPutWithTimeout(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnRemoveAll(t, cfg)
	}
}

func testInvalidateOnRemoveAll(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnClear(t, cfg)
	}
}

func testInvalidateOnClear(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnEvictAll(t, cfg)
	}
}

func testInvalidateOnEvictAll(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnReplace(t, cfg)
	}
}

func testInvalidateOnReplace(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnReplaceIfSame(t, cfg)
	}
}

func testInvalidateOnReplaceIfSame(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	mp, err := client.GetMap("testName2")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.ReplaceIfSame("key", "value2", "value")
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnSet(t *testing.T) {
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnSet(t, cfg)
	}
}

func testInvalidateOnSet(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnSetWithTTL(t, cfg)
	}
}

func testInvalidateOnSetWithTTL(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnPutAll(t, cfg)
	}
}

func testInvalidateOnPutAll(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnPutTransient(t, cfg)
	}
}

func testInvalidateOnPutTransient(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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

func TestInvalidateOnPutIfAbsent(t *testing.T) {
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnPutIfAbsent(t, cfg)
	}
}

func testInvalidateOnPutIfAbsent(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	mp.Put("key", "value")
	mp.Get("key")
	cache := GetNearCacheFromMap(mp)
	assert.Equal(t, cache.Size(), 1)

	mp.PutIfAbsent("key", "value2")
	assert.Equal(t, cache.Size(), 0)
}

func TestInvalidateOnExecuteOnKey(t *testing.T) {
	for _, cfg := range CreateNearCacheConfigs() {
		testInvalidateOnExecuteOnKey(t, cfg)
	}
}

func testInvalidateOnExecuteOnKey(t *testing.T, cfg *config.Config) {
	client, err := hazelcast.NewClientWithConfig(cfg)
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

func TestNearCacheStoreIsAlwaysLessThanMaxSize(t *testing.T) {
	nearCacheSize := 20
	config := CreateConfigWithDefaultNearCache()
	config.NearCacheConfig().SetMaxEntryCount(int32(nearCacheSize))
	client, err := hazelcast.NewClientWithConfig(config)
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)
	cache := GetNearCacheFromMap(mp)
	for i := 0; i < nearCacheSize*10; i++ {
		mp.Put(strconv.Itoa(i), i)
		mp.Get(strconv.Itoa(i))
		mp.Get(strconv.Itoa(i))
		currentSize := cache.Size()
		if currentSize > nearCacheSize {
			t.Error("currentSize of near cache is greater than max size", currentSize, nearCacheSize)
		}
	}

	client.Shutdown()
}

func TestNearCacheGetWhenRecordExpired(t *testing.T) {
	config := CreateConfigWithDefaultNearCache()
	config.NearCacheConfig().SetTimeToLive(10 * time.Millisecond)
	client, err := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)
	mp.Put("key", "value")
	mp.Get("key")

	cache := GetNearCacheFromMap(mp)
	test.AssertEventually(t, func() bool {
		value := cache.Get("key")
		return value == nil
	})

}

type dummy struct {
	d int32
}

func TestNonserializableKey(t *testing.T) {
	cfg := CreateConfigWithDefaultNearCache()
	cfg.NearCacheConfig().SetSerializeKeys(true)
	client, err := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	mp, err := client.GetMap("testName")
	assert.NoError(t, err)

	_, err = mp.Put(dummy{}, "0")
	assert.Error(t, err)
	_, err = mp.Get(dummy{})
	assert.Error(t, err)
	_, err = mp.PutIfAbsent(dummy{}, "0")
	assert.Error(t, err)
	err = mp.PutTransient(dummy{}, "0", 1*time.Second)
	assert.Error(t, err)
	_, err = mp.Replace(dummy{}, "0")
	assert.Error(t, err)
	_, err = mp.ReplaceIfSame(dummy{}, "0", "")
	assert.Error(t, err)
	_, err = mp.Remove(dummy{})
	assert.Error(t, err)
	_, err = mp.RemoveIfSame(dummy{}, "0")
	assert.Error(t, err)
	_, err = mp.ContainsKey(dummy{})
	assert.Error(t, err)
	_, err = mp.Evict(dummy{})
	assert.Error(t, err)
	err = mp.Set(dummy{}, "0")
	assert.Error(t, err)
	err = mp.SetWithTTL(dummy{}, "0", time.Second)
	assert.Error(t, err)
	_, err = mp.ExecuteOnKey(dummy{}, "0")
	assert.Error(t, err)
	_, err = mp.TryRemove(dummy{}, time.Second)
	assert.Error(t, err)
	_, err = mp.TryPut(dummy{}, "0")
	assert.Error(t, err)
	_, err = mp.TryPutWithTimeout(dummy{}, "0", time.Second)
	assert.Error(t, err)

}

func preventFromBeingIdle(key interface{}, mp core.Map) {
	mp.Get(key)
}
