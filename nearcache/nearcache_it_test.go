//go:build hazelcastinternal && hazelcastinternaltest
// +build hazelcastinternal,hazelcastinternaltest

/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nearcache_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	inearcache "github.com/hazelcast/hazelcast-go-client/internal/nearcache"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	nearCacheDefaultRecordCount = 1000
	maxCacheSize                = 1000
	maxTTLSeconds               = 2
	maxIdleSeconds              = 1
)

func TestSmokeNearCachePopulation(t *testing.T) {
	// ported from: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#smoke_near_cache_population
	t.Skipf("this test is flaky, to be addressed before the release")
	tcx := it.MapTestContext{
		T: t,
		ConfigCallback: func(tcx it.MapTestContext) {
			ncc := nearcache.Config{Name: tcx.MapName}
			ncc.SetInvalidateOnChange(true)
			tcx.Config.AddNearCache(ncc)
		},
		Before: expirationBefore,
		After:  expirationAfter,
	}
	tcx.Tester(func(tcx it.MapTestContext) {
		m := tcx.M
		t := tcx.T
		ctx := context.Background()
		// assert cluster size
		it.Eventually(t, func() bool {
			ci := hz.NewClientInternal(tcx.Client)
			mems := ci.OrderedMembers()
			t.Logf("member count: %d, expected: %d", len(mems), it.MemberCount())
			return len(mems) == it.MemberCount()
		})
		const mapSize = 1000
		cls := tcx.Cluster
		// 2. populate server side map
		for i := 0; i < mapSize; i++ {
			v := strconv.Itoa(i)
			it.MapSetOnServer(cls.ClusterID, tcx.MapName, v, v)
		}
		// 4. populate client Near Cache
		for i := int32(0); i < mapSize; i++ {
			v := it.MustValue(m.Get(ctx, i))
			require.Equal(t, i, v)
		}
		// 5. assert number of entries in client Near Cache
		nca := hz.MakeNearCacheAdapterFromMap(m).(it.NearCacheAdapter)
		require.Equal(t, mapSize, nca.Size())
	})
}

func TestGetAllChecksNearCacheFirst(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testGetAllChecksNearCacheFirst
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, false)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		ctx := context.Background()
		var keys []interface{}
		var target []types.Entry
		const size = 1003
		for i := int64(0); i < size; i++ {
			if _, err := m.Put(ctx, i, i); err != nil {
				t.Fatal(err)
			}
			keys = append(keys, i)
			target = append(target, types.Entry{Key: i, Value: i})
		}
		// populate Near Cache
		for i := int64(0); i < size; i++ {
			v, err := m.Get(ctx, i)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, i, v)
		}
		// GetAll() generates the Near Cache hits
		vs, err := m.GetAll(ctx, keys...)
		if err != nil {
			t.Fatal(err)
		}
		require.ElementsMatch(t, target, vs)
		stats := m.LocalMapStats().NearCacheStats
		require.Equal(t, int64(size), stats.OwnedEntryCount)
		require.Equal(t, int64(size), stats.Hits)
	})
}

func TestGetAllPopulatesNearCache(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testGetAllPopulatesNearCache
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, false)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		ctx := context.Background()
		var keys []interface{}
		var target []types.Entry
		const size = 1214
		for i := int64(0); i < size; i++ {
			if _, err := m.Put(ctx, i, i); err != nil {
				t.Fatal(err)
			}
			keys = append(keys, i)
			target = append(target, types.Entry{Key: i, Value: i})
		}
		vs, err := m.GetAll(ctx, keys...)
		if err != nil {
			t.Fatal(err)
		}
		require.ElementsMatch(t, target, vs)
		stats := m.LocalMapStats().NearCacheStats
		require.Equal(t, int64(size), stats.OwnedEntryCount)
	})
}

func TestGetNearCacheStatsBeforePopulation(t *testing.T) {
	// ported from: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testGetNearCacheStatsBeforePopulation
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, false)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		for i := int64(0); i < 101; i++ {
			if _, err := m.Put(context.Background(), i, i); err != nil {
				t.Fatal(err)
			}
		}
		stats := m.LocalMapStats().NearCacheStats
		assert.NotEqual(t, nearcache.Stats{}, stats)
	})
}

func TestNearCacheClearFromClient(t *testing.T) {
	// ported from: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCache_clearFromClient
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		ctx := context.Background()
		const size = int32(147)
		populateMap(tcx, size)
		populateNearCache(tcx, size)
		if err := m.Clear(ctx); err != nil {
			t.Fatal(err)
		}
		// Near Cache should be empty
		for i := int32(0); i < size; i++ {
			v, err := m.Get(ctx, i)
			if err != nil {
				t.Fatal(err)
			}
			require.Nil(t, v)
		}
	})
}

func TestNearCacheClearFromRemote(t *testing.T) {
	// ported from: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCache_clearFromRemote
	tcx := newNearCacheMapTestContextWithExpiration(t, nearcache.InMemoryFormatObject, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		ctx := context.Background()
		const size = int32(147)
		populateMap(tcx, size)
		populateNearCache(tcx, size)
		tcx.ExecuteScript(ctx, fmt.Sprintf(`
			var map = instance_0.getMap("%s");
        	map.clear();
		`, tcx.MapName))
		// Near Cache should be empty
		it.Eventually(t, func() bool {
			for i := int32(0); i < size; i++ {
				fmt.Println("i:", i)
				v, err := m.Get(ctx, i)
				if err != nil {
					panic(err)
				}
				if v != nil {
					return false
				}
			}
			return true
		})
	})
}

func TestNearCacheMisses(t *testing.T) {
	// ported from: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCacheMisses
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, false)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		const expectedCacheMisses = int64(1321)
		for i := int64(0); i < expectedCacheMisses; i++ {
			k := fmt.Sprintf("NOT_THERE%d", i)
			if _, err := m.Get(context.Background(), k); err != nil {
				t.Fatal(err)
			}
		}
		stats := m.LocalMapStats().NearCacheStats
		assert.Equal(t, expectedCacheMisses, stats.Misses)
		assert.Equal(t, expectedCacheMisses, stats.OwnedEntryCount)
	})
}

func TestNearCachePopulatedAndHitsGenerated(t *testing.T) {
	// ported from: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCachePopulatedAndHitsGenerated
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, false)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		ctx := context.Background()
		const size = 1278
		populateMap(tcx, size)
		populateNearCache(tcx, size)
		// generate Near Cache hits
		for i := int32(0); i < size; i++ {
			v := it.MustValue(m.Get(ctx, i))
			require.Equal(t, i, v)
		}
		stats := m.LocalMapStats().NearCacheStats
		require.Equal(t, int64(size), stats.OwnedEntryCount)
		require.Equal(t, int64(size), stats.Hits)
	})
}

func TestNearCachePopulatedAndHitsGenerated_withInterleavedCacheHitGeneration(t *testing.T) {
	// ported from: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCachePopulatedAndHitsGenerated_withInterleavedCacheHitGeneration
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, false)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		ctx := context.Background()
		const size = 1278
		for i := int32(0); i < size; i++ {
			it.MustValue(m.Put(ctx, i, i))
			// populate Near Cache
			it.MustValue(m.Get(ctx, i))
			// generate Near Cache hits
			it.MustValue(m.Get(ctx, i))
		}
		stats := m.LocalMapStats().NearCacheStats
		require.Equal(t, int64(size), stats.OwnedEntryCount)
		require.Equal(t, int64(size), stats.Hits)
	})
}

func TestRemovedKeyValueNotInNearCache(t *testing.T) {
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		ctx := context.Background()
		const size = 1247
		populateMap(tcx, size)
		populateNearCache(tcx, size)
		for i := int32(0); i < size; i++ {
			it.MustValue(m.Remove(ctx, i))
			require.Nil(t, it.MustValue(m.Get(ctx, i)))
		}
	})
}

func TestMapRemove_WithNearCache(t *testing.T) {
	// ported from: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testMapRemove_WithNearCache
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatBinary, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		const size = int32(1113)
		populateMap(tcx, size)
		populateNearCache(tcx, size)
		for i := int32(0); i < size; i++ {
			if _, err := m.Remove(context.Background(), i); err != nil {
				t.Fatal(err)
			}
		}
		stats := m.LocalMapStats().NearCacheStats
		assert.Equal(t, int64(0), stats.OwnedEntryCount)
		assert.Equal(t, int64(size), stats.Misses)
	})
}

func TestNearCacheTTLExpiration(t *testing.T) {
	// ported from: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCacheTTLExpiration
	tcx := it.MapTestContext{
		T: t,
		ConfigCallback: func(tcx it.MapTestContext) {
			ncc := nearcache.Config{
				Name:              tcx.MapName,
				TimeToLiveSeconds: maxTTLSeconds,
			}
			ncc.SetInvalidateOnChange(false)
			tcx.Config.AddNearCache(ncc)
		},
	}
	tcx.Tester(ttlMaxIdleTester)
}

func ttlMaxIdleTester(tcx it.MapTestContext) {
	populateServerMap(tcx, maxCacheSize)
	populateNearCache(tcx, maxCacheSize)
	assertNearCacheExpiration(tcx, maxCacheSize)
}

// TestWhenGetIsUsedThenNearCacheShouldBePopulated checks that the Near Cache is populated when Get is used.
// And also the NearCacheStats are calculated correctly.
func TestWhenGetIsUsedThenNearCacheShouldBePopulated(t *testing.T) {
	// ported from: com.hazelcast.internal.nearcache.impl.AbstractNearCacheBasicTest#whenGetIsUsed_thenNearCacheShouldBePopulated()
	clientCacheNearCacheBasicSlowRunner(t, func(tcx *it.NearCacheTestContext, size int64, valueFmt string) {
		tcx.PopulateNearCacheWithGet(size, valueFmt)
	})
}

func TestNearCacheGet(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testGetAsync
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, false)
	tcx.Tester(func(tcx it.MapTestContext) {
		m := tcx.M
		t := tcx.T
		const size = int32(1009)
		ctx := context.Background()
		populateMap(tcx, size)
		populateNearCache(tcx, size)
		// generate near cache hits
		for i := int32(0); i < size; i++ {
			v := it.MustValue(m.Get(ctx, i))
			require.Equal(t, i, v)
		}
		stats := m.LocalMapStats().NearCacheStats
		require.Equal(t, int64(size), stats.Hits)
		require.Equal(t, int64(size), stats.OwnedEntryCount)
	})
}

func TestNearCacheInvalidateOnChange(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCacheInvalidateOnChange
	tcx := newNearCacheMapTestContextWithExpiration(t, nearcache.InMemoryFormatObject, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		m := tcx.M
		t := tcx.T
		ctx := context.Background()
		const size = 118
		populateServerMapWithString(ctx, tcx, size)
		// populate Near Cache
		populateNearCacheWithString(tcx, size)
		oec := m.LocalMapStats().NearCacheStats.OwnedEntryCount
		require.Equal(t, int64(size), oec)
		// invalidate Near Cache from server side
		populateServerMapWithString(ctx, tcx, size)
		it.Eventually(t, func() bool {
			oec := m.LocalMapStats().NearCacheStats.OwnedEntryCount
			t.Logf("OEC: %d", oec)
			return oec == 0
		})
	})
}

func TestNearCacheInvalidation_WithLFU_whenMaxSizeExceeded(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCacheInvalidation_WithLFU_whenMaxSizeExceeded
	ncc := makeNearCacheConfigWithEviction(nearcache.EvictionPolicyLFU)
	assertNearCacheInvalidationWhenMaxSizeExceeded(t, ncc)
}

func TestNearCacheInvalidation_WithLRU_whenMaxSizeExceeded(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCacheInvalidation_WithLRU_whenMaxSizeExceeded
	ncc := makeNearCacheConfigWithEviction(nearcache.EvictionPolicyLRU)
	assertNearCacheInvalidationWhenMaxSizeExceeded(t, ncc)
}

func TestNearCacheInvalidation_WithNone_whenMaxSizeExceeded(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCacheInvalidation_WithNone_whenMaxSizeExceeded
	tcx := it.MapTestContext{
		T: t,
		ConfigCallback: func(tcx it.MapTestContext) {
			ncc := makeNearCacheConfigWithEviction(nearcache.EvictionPolicyNone)
			tcx.Config.AddNearCache(ncc)
		},
	}
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		const mapSize = maxCacheSize * 2
		populateMap(tcx, mapSize)
		populateNearCache(tcx, mapSize)
		it.Eventually(t, func() bool {
			oec := tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount
			t.Logf("OEC: %d, %t", oec, oec == maxCacheSize)
			return oec == maxCacheSize
		})
	})
}

func TestNearCacheInvalidationWithRandom_whenMaxSizeExceeded(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCacheInvalidation_WithRandom_whenMaxSizeExceeded
	ncc := makeNearCacheConfigWithEviction(nearcache.EvictionPolicyRandom)
	assertNearCacheInvalidationWhenMaxSizeExceeded(t, ncc)
}

func TestNearCacheIsRemoved_afterMapDestroy(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCacheIsRemoved_afterMapDestroy
	tcx := newNearCacheMapTestContextWithExpiration(t, nearcache.InMemoryFormatObject, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		ctx := context.Background()
		const mapSize = 1000
		populateMap(tcx, mapSize)
		populateNearCache(tcx, mapSize)
		oec := m.LocalMapStats().NearCacheStats.OwnedEntryCount
		require.Equal(t, int64(mapSize), oec)
		it.Must(m.Destroy(ctx))
		m = it.MustValue(tcx.Client.GetMap(ctx, tcx.MapName)).(*hz.Map)
		oec = m.LocalMapStats().NearCacheStats.OwnedEntryCount
		require.Equal(t, int64(0), oec)
	})
}

func makeNearCacheConfigWithEviction(policy nearcache.EvictionPolicy) nearcache.Config {
	ec := nearcache.EvictionConfig{}
	ec.SetPolicy(policy)
	ec.SetSize(maxCacheSize)
	ncc := nearcache.Config{
		Name:     "*",
		Eviction: ec,
	}
	ncc.SetInvalidateOnChange(true)
	return ncc
}

func assertNearCacheInvalidationWhenMaxSizeExceeded(t *testing.T, ncc nearcache.Config) {
	// see: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#assertNearCacheInvalidation_whenMaxSizeExceeded
	tcx := it.MapTestContext{
		T: t,
		ConfigCallback: func(tcx it.MapTestContext) {
			tcx.Config.AddNearCache(ncc)
		},
	}
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		populateMap(tcx, maxCacheSize)
		populateNearCache(tcx, maxCacheSize)
		// trigger eviction
		it.MustValue(m.Put(context.Background(), int32(0), int32(0)))
		it.Eventually(t, func() bool {
			oec := m.LocalMapStats().NearCacheStats.OwnedEntryCount
			return oec < maxCacheSize
		})
	})
}

func TestNearCacheContainsKey(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCacheContainsKey
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		m := tcx.M
		t := tcx.T
		ctx := context.Background()
		key := "key"
		value := "value"
		it.MustValue(m.Put(ctx, key, value))
		v := it.MustValue(m.Get(ctx, key))
		require.Equal(t, v, value)
		require.True(t, it.MustBool(m.ContainsKey(ctx, key)))
	})
}

func TestNearCacheContainsKey_AfterRemove(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCacheContainsKey_afterRemove
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		m := tcx.M
		t := tcx.T
		ctx := context.Background()
		key := "key"
		value := "value"
		it.MustValue(m.Put(ctx, key, value))
		v := it.MustValue(m.Get(ctx, key))
		it.MustValue(m.Remove(ctx, key))
		require.Equal(t, v, value)
		require.False(t, it.MustBool(m.ContainsKey(ctx, key)))
	})
}

func TestNearCacheContainsKey_whenKeyAbsent(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCacheContainsKey_whenKeyAbsent
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		ctx := context.Background()
		require.False(t, it.MustBool(tcx.M.ContainsKey(ctx, "NOT_THERE")))
	})
}

func TestNearCacheContainsNullKey(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testNearCacheContainsNullKey
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		_, err := tcx.M.ContainsKey(context.Background(), nil)
		require.Error(t, err)
	})

}

type mapTestCase struct {
	name string
	f    func(context.Context, it.MapTestContext, int32)
}

func TestAfterRemoveNearCacheIsInvalidated(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterRemoveNearCacheIsInvalidated
	testCases := []mapTestCase{
		{
			name: "Remove",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				v, err := tcx.M.Remove(ctx, i)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.Equal(tcx.T, v, i)
			},
		},
		{
			name: "RemoveIfSame",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				b, err := tcx.M.RemoveIfSame(ctx, i, i)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.True(tcx.T, b)
			},
		},
	}
	invalidationRunner(t, testCases)
}

func TestAfterDeleteNearCacheIsInvalidated(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterDeleteNearCacheIsInvalidated
	testCases := []mapTestCase{
		{
			name: "Delete",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				if err := tcx.M.Delete(ctx, i); err != nil {
					tcx.T.Fatal(err)
				}
			},
		},
	}
	invalidationRunner(t, testCases)
}

func TestAfterPutNearCacheIsInvalidated(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterPutAsyncNearCacheIsInvalidated
	testCases := []mapTestCase{
		{
			name: "Put",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				v, err := tcx.M.Put(ctx, i, i)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.Equal(t, i, v)
			},
		},
		{
			name: "PutWithTTL",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				v, err := tcx.M.PutWithTTL(ctx, i, i, 10*time.Second)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.Equal(t, i, v)
			},
		},
		{
			name: "PutTransient",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				if err := tcx.M.PutTransient(ctx, i, i); err != nil {
					tcx.T.Fatal(err)
				}
			},
		},
		{
			name: "PutTransientWithTTL",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				if err := tcx.M.PutTransientWithTTL(ctx, i, i, 10*time.Second); err != nil {
					tcx.T.Fatal(err)
				}
			},
		},
		{
			name: "PutTransientWithTTLAndMaxIdle",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				if err := tcx.M.PutTransientWithTTLAndMaxIdle(ctx, i, i, 10*time.Second, 5*time.Second); err != nil {
					tcx.T.Fatal(err)
				}
			},
		},
		{
			name: "PutIfAbsent",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				v, err := tcx.M.PutIfAbsent(ctx, i, i)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.Equal(t, i, v)
			},
		},
		{
			name: "PutIfAbsentWithTTL",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				v, err := tcx.M.PutIfAbsentWithTTL(ctx, i, i, 10*time.Second)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.Equal(t, i, v)
			},
		},
		{
			name: "PutIfAbsentWithTTLAndMaxIdle",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				v, err := tcx.M.PutIfAbsentWithTTLAndMaxIdle(ctx, i, i, 10*time.Second, 5*time.Second)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.Equal(t, i, v)
			},
		},
	}
	invalidationRunner(t, testCases)
}

func TestAfterPulAllNearCacheIsInvalidated(t *testing.T) {
	// this test does not exist in the reference implementation
	tcx := newNearCacheMapTestContextWithExpiration(t, nearcache.InMemoryFormatBinary, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		const size = 1000
		keys := populateMapWithString(tcx, size)
		populateNearCacheWithString(tcx, size)
		oec := m.LocalMapStats().NearCacheStats.OwnedEntryCount
		require.Equal(t, int64(size), oec)
		entries := []types.Entry{}
		for _, k := range keys {
			entries = append(entries, types.Entry{Key: k, Value: k})
		}
		it.Must(m.PutAll(context.Background(), entries...))
		oec = m.LocalMapStats().NearCacheStats.OwnedEntryCount
		require.Equal(t, int64(0), oec)
	})
}

func TestAfterReplaceNearCacheIsInvalidated(t *testing.T) {
	// no corresponding test in the reference implementation
	testCases := []mapTestCase{
		{
			name: "Replace",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				v := it.MustValue(tcx.M.Replace(ctx, i, i))
				require.Equal(t, i, v)
			},
		},
		{
			name: "ReplaceIfSame",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				v := it.MustBool(tcx.M.ReplaceIfSame(ctx, i, i, i))
				require.Equal(t, true, v)
			},
		},
	}
	invalidationRunner(t, testCases)
}

func TestAfterSetNearCacheIsInvalidated(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterSetAsyncNearCacheIsInvalidated
	testCases := []mapTestCase{
		{
			name: "Set",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				if err := tcx.M.Set(ctx, i, i); err != nil {
					tcx.T.Fatal(err)
				}
			},
		},
		{
			name: "SetWithTTL",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				if err := tcx.M.SetWithTTL(ctx, i, i, 1*time.Second); err != nil {
					tcx.T.Fatal(err)
				}
			},
		},
		{
			name: "SetWithTTLAndMaxIdle",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				if err := tcx.M.SetWithTTLAndMaxIdle(ctx, i, i, 1*time.Second, 2*time.Second); err != nil {
					tcx.T.Fatal(err)
				}
			},
		},
	}
	invalidationRunner(t, testCases)
}

func TestAfterEvictNearCacheIsInvalidated(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterEvictNearCacheIsInvalidated
	testCases := []mapTestCase{
		{
			name: "Evict",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				b, err := tcx.M.Evict(ctx, i)
				if err != nil {
					tcx.T.Fatal(err)
				}
				assert.True(t, true, b)
			},
		},
	}
	invalidationRunner(t, testCases)
}

func TestAfterEvictAllNearCacheIsInvalidated(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterEvictAllNearCacheIsInvalidated
	tcx := newNearCacheMapTestContextWithExpiration(t, nearcache.InMemoryFormatBinary, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		const size = int32(1000)
		ctx := context.Background()
		populateMap(tcx, size)
		populateNearCache(tcx, size)
		require.Equal(t, int64(size), tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount)
		if err := m.EvictAll(ctx); err != nil {
			t.Fatal(err)
		}
		require.Equal(t, int64(0), tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount)
	})
}

func TestAfterExecuteOnKeyKeyIsInvalidatedFromNearCache(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterExecuteOnKeyKeyIsInvalidatedFromNearCache
	tcx := newNearCacheMapTestContextWithExpiration(t, nearcache.InMemoryFormatBinary, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		const size = int32(1000)
		ctx := context.Background()
		populateMap(tcx, size)
		populateNearCache(tcx, size)
		require.Equal(t, int64(size), tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount)
		randomKey := int32(rand.Intn(int(size)))
		// using a different entry processor
		_, err := m.ExecuteOnKey(ctx, &SimpleEntryProcessor{value: "value"}, randomKey)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, int64(size-1), tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount)
	})
}

func TestAfterExecuteOnKeysKeysAreInvalidatedFromNearCache(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterExecuteOnKeyKeyIsInvalidatedFromNearCache
	tcx := newNearCacheMapTestContextWithExpiration(t, nearcache.InMemoryFormatBinary, true)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		const size = int32(1000)
		ctx := context.Background()
		populateMap(tcx, size)
		populateNearCache(tcx, size)
		require.Equal(t, int64(size), tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount)
		const keyCount = size / 10
		keySet := make(map[int32]struct{})
		for len(keySet) < int(keyCount) {
			key := int32(rand.Intn(int(size)))
			keySet[key] = struct{}{}
		}
		keys := make([]interface{}, 0, keyCount)
		for k := range keySet {
			keys = append(keys, k)
		}
		// using a different entry processor
		_, err := m.ExecuteOnKeys(ctx, &SimpleEntryProcessor{value: "value"}, keys...)
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, int64(size-keyCount), tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount)
	})
}

func TestAfterLoadAllWithDefinedKeysNearCacheIsInvalidated(t *testing.T) {
	// see: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterLoadAllWithDefinedKeysNearCacheIsInvalidated
	testCases := []struct {
		name string
		f    func(ctx context.Context, tcx it.MapTestContext, keys []interface{}) error
	}{
		{
			name: "LoadAllWithoutReplacing",
			f: func(ctx context.Context, tcx it.MapTestContext, keys []interface{}) error {
				return tcx.M.LoadAllWithoutReplacing(ctx, keys...)
			},
		},
		{
			name: "LoadAllReplacing",
			f: func(ctx context.Context, tcx it.MapTestContext, keys []interface{}) error {
				return tcx.M.LoadAllReplacing(ctx, keys...)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tcx := newNearCacheMapTestContextWithExpiration(t, nearcache.InMemoryFormatBinary, true)
			// note that the following is hardcoded
			tcx.MapName = "test-map"
			tcx.Tester(func(tcx it.MapTestContext) {
				t := tcx.T
				const mapSize = 1000
				ctx := context.Background()
				keys := populateMapWithString(tcx, mapSize)
				populateNearCacheWithString(tcx, mapSize)
				if err := tc.f(ctx, tcx, keys); err != nil {
					t.Fatal(err)
				}
				require.Equal(t, int64(0), tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount)
			})
		})
	}
}

func TestAfterTryRemoveNearCacheIsInvalidated(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterTryRemoveNearCacheIsInvalidated
	testCases := []mapTestCase{
		{
			name: "TryRemove",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				v, err := tcx.M.TryRemove(ctx, i)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.True(t, v.(bool))
			},
		},
		{
			name: "TryRemoveWithTimeout",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				v, err := tcx.M.TryRemoveWithTimeout(ctx, i, 10*time.Second)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.True(t, v.(bool))
			},
		},
	}
	invalidationRunner(t, testCases)
}

func TestAfterTryPutNearCacheIsInvalidated(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterTryPutNearCacheIsInvalidated
	testCases := []mapTestCase{
		{
			name: "TryPut",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				v, err := tcx.M.TryPut(ctx, i, i)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.Equal(tcx.T, true, v)
			},
		},
		{
			name: "TryPutWithTimeout",
			f: func(ctx context.Context, tcx it.MapTestContext, i int32) {
				v, err := tcx.M.TryPutWithTimeout(ctx, i, i, 10*time.Second)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.Equal(tcx.T, true, v)
			},
		},
	}
	invalidationRunner(t, testCases)
}

func TestMemberLoadAllInvalidatesClientNearCache(t *testing.T) {
	t.Skipf("this test is currently flaky")
	// ported from: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testMemberLoadAll_invalidates_clientNearCache
	f := func(tcx it.MapTestContext, size int32) string {
		mode := "unisocket"
		if tcx.Smart {
			mode = "smart"
		}
		return fmt.Sprintf(`
			var map = instance_0.getMap("test-map-%s");
        	map.loadAll(true);
		`, mode)
	}
	memberInvalidatesClientNearCache(t, true, f)
}

func TestMemberPutAllInvalidatesClientNearCache(t *testing.T) {
	// ported from: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testMemberPutAll_invalidates_clientNearCache
	f := func(tcx it.MapTestContext, size int32) string {
		return fmt.Sprintf(`
			var items = new java.util.HashMap(%[1]d);
			for (var i = 0; i < %[1]d; i++) {
		       items.put(""+i, ""+i);
			}
			var map = instance_0.getMap("%[2]s");
        	map.putAll(items);
		`, size, tcx.MapName)
	}
	memberInvalidatesClientNearCache(t, false, f)
}

func TestMemberSetAllInvalidatesClientNearCache(t *testing.T) {
	// see: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testMemberSetAll_invalidates_clientNearCache
	f := func(tcx it.MapTestContext, size int32) string {
		return fmt.Sprintf(`
			var items = new java.util.HashMap(%[1]d);
			for (var i = 0; i < %[1]d; i++) {
		       items.put(""+i, ""+i);
			}
			var map = instance_0.getMap("%[2]s");
        	map.setAll(items);
		`, size, tcx.MapName)
	}
	memberInvalidatesClientNearCache(t, false, f)
}

func memberInvalidatesClientNearCache(t *testing.T, useTestMap bool, makeScript func(tcx it.MapTestContext, size int32) string) {
	// ported from: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testMemberLoadAll_invalidates_clientNearCache
	tcx := newNearCacheMapTestContextWithExpiration(t, nearcache.InMemoryFormatBinary, true)
	if useTestMap {
		// hardcoded map name for LoadAll to work
		tcx.NameMaker = func(p ...string) string {
			suffix := strings.Join(p, "-")
			return fmt.Sprintf("test-map-%s", suffix)
		}
	}
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		const mapSize = 1000
		// ignoring returned keys
		_ = populateMapWithString(tcx, mapSize)
		populateNearCacheWithString(tcx, mapSize)
		script := makeScript(tcx, mapSize)
		tcx.ExecuteScript(context.Background(), script)
		it.Eventually(t, func() bool {
			oec := tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount
			t.Logf("OEC: %d", oec)
			return 0 == oec
		})
	})
}

func clientCacheNearCacheBasicSlowRunner(t *testing.T, f func(tcx *it.NearCacheTestContext, size int64, valueFmt string)) {
	testCases := []struct {
		inMemoryFmt   nearcache.InMemoryFormat
		serializeKeys bool
	}{
		{inMemoryFmt: nearcache.InMemoryFormatBinary, serializeKeys: true},
		{inMemoryFmt: nearcache.InMemoryFormatBinary, serializeKeys: false},
		{inMemoryFmt: nearcache.InMemoryFormatObject, serializeKeys: true},
		{inMemoryFmt: nearcache.InMemoryFormatObject, serializeKeys: false},
	}
	for _, tc := range testCases {
		testName := fmt.Sprintf("inMemoryFmt %s serializeKeys %t", inMemoryFmtToString(tc.inMemoryFmt), tc.serializeKeys)
		t.Run(testName, func(t *testing.T) {
			ncc := nearcache.Config{
				Name:           "*",
				InMemoryFormat: tc.inMemoryFmt,
				SerializeKeys:  tc.serializeKeys,
			}
			ncc.SetInvalidateOnChange(false)
			configCB := func(tcx it.MapTestContext) {
				tcx.Config.AddNearCache(ncc)
			}
			valueFmt := "value-%d"
			mtcx := &it.MapTestContext{
				T:              t,
				ConfigCallback: configCB,
			}
			mtcx.Tester(func(mtcx it.MapTestContext) {
				nca := hz.MakeNearCacheAdapterFromMap(mtcx.M).(it.NearCacheAdapter)
				ci := hz.NewClientInternal(mtcx.Client)
				tcx := it.NewNearCacheTestContext(mtcx.T, nca, mtcx.M, &ncc, ci.SerializationService())
				// assert that the Near Cache is empty
				tcx.PopulateNearCacheDataAdapter(nearCacheDefaultRecordCount, valueFmt)
				tcx.RequireNearCacheSize(0)
				tcx.AssertNearCacheStats(0, 0, 0)
				// populate the Near Cache
				f(tcx, nearCacheDefaultRecordCount, valueFmt)
				tcx.RequireNearCacheSize(nearCacheDefaultRecordCount)
				tcx.AssertNearCacheStats(nearCacheDefaultRecordCount, 0, nearCacheDefaultRecordCount)
				// generate Near Cache hits
				f(tcx, nearCacheDefaultRecordCount, valueFmt)
				tcx.RequireNearCacheSize(nearCacheDefaultRecordCount)
				tcx.AssertNearCacheStats(nearCacheDefaultRecordCount, nearCacheDefaultRecordCount, nearCacheDefaultRecordCount)
				tcx.AssertNearCacheContent(nca, nearCacheDefaultRecordCount, valueFmt)
				// since non-pointer values are copid, the following assertion doesn't hold
				// assertNearCacheReferences
			})
		})
	}
}

func invalidationRunner(t *testing.T, testCases []mapTestCase) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tcx := newNearCacheMapTestContextWithExpiration(t, nearcache.InMemoryFormatBinary, true)
			tcx.Tester(func(tcx it.MapTestContext) {
				t := tcx.T
				const size = int32(1000)
				ctx := context.Background()
				populateMap(tcx, size)
				populateNearCache(tcx, size)
				require.Equal(t, int64(size), tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount)
				for i := int32(0); i < size; i++ {
					tc.f(ctx, tcx, i)
				}
				require.Equal(t, int64(0), tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount)
			})
		})
	}
}

func inMemoryFmtToString(fmt nearcache.InMemoryFormat) string {
	switch fmt {
	case nearcache.InMemoryFormatBinary:
		return "binary"
	case nearcache.InMemoryFormatObject:
		return "object"
	default:
		return "UNKNOWN"
	}
}

func populateMap(tcx it.MapTestContext, size int32) {
	for i := int32(0); i < size; i++ {
		if _, err := tcx.M.Put(context.Background(), i, i); err != nil {
			panic(err)
		}
	}
}

func populateMapWithString(tcx it.MapTestContext, size int) []interface{} {
	keys := []interface{}{}
	for i := 0; i < size; i++ {
		key := strconv.Itoa(i)
		value := strconv.Itoa(i)
		if _, err := tcx.M.Put(context.Background(), key, value); err != nil {
			tcx.T.Fatal(err)
		}
		keys = append(keys, key)
	}
	return keys
}

func populateNearCacheWithString(tcx it.MapTestContext, size int) {
	for i := 0; i < size; i++ {
		key := strconv.Itoa(i)
		v, err := tcx.M.Get(context.Background(), key)
		if err != nil {
			tcx.T.Fatal(err)
		}
		require.Equal(tcx.T, v, key)
	}
}

func populateServerMap(tcx it.MapTestContext, size int32) {
	for i := int32(0); i < size; i++ {
		v := strconv.Itoa(int(i))
		it.MapSetOnServer(tcx.Cluster.ClusterID, tcx.MapName, v, v)
	}
}

func populateServerMapWithString(ctx context.Context, tcx it.MapTestContext, size int) {
	tcx.ExecuteScript(ctx, fmt.Sprintf(`
			var serverMap = instance_0.getMap("%s");
			for (var i = 0; i < %d; i++) {
				serverMap.put(""+i, ""+i);
			}
		`, tcx.MapName, size))
}

func populateNearCache(tcx it.MapTestContext, size int32) {
	for i := int32(0); i < size; i++ {
		v, err := tcx.M.Get(context.Background(), i)
		if err != nil {
			tcx.T.Fatal(err)
		}
		require.Equal(tcx.T, v, i)
	}
}

func newNearCacheMapTestContext(t *testing.T, fmt nearcache.InMemoryFormat, invalidate bool) it.MapTestContext {
	return it.MapTestContext{
		T: t,
		ConfigCallback: func(tcx it.MapTestContext) {
			ncc := nearcache.Config{
				Name:           tcx.MapName,
				InMemoryFormat: fmt,
			}
			ncc.SetInvalidateOnChange(invalidate)
			tcx.Config.AddNearCache(ncc)
		},
	}
}

func newNearCacheMapTestContextWithExpiration(t *testing.T, fmt nearcache.InMemoryFormat, invalidate bool) it.MapTestContext {
	return it.MapTestContext{
		T: t,
		ConfigCallback: func(tcx it.MapTestContext) {
			ncc := nearcache.Config{
				Name:           tcx.MapName,
				InMemoryFormat: fmt,
			}
			ncc.SetInvalidateOnChange(invalidate)
			tcx.Config.AddNearCache(ncc)
		},
		Before: expirationBefore,
		After:  expirationAfter,
	}
}

func assertNearCacheExpiration(tcx it.MapTestContext, size int32) {
	t := tcx.T
	m := tcx.M
	it.Eventually(t, func() bool {
		nca := hz.MakeNearCacheAdapterFromMap(m).(it.NearCacheAdapter)
		stats := m.LocalMapStats().NearCacheStats
		// make assertions over near cache's backing map size.
		t.Logf(
			"size: %d, OEC: %d, OEMC: %d, ex: %d, ev: %d",
			nca.Size(),
			stats.OwnedEntryCount,
			stats.OwnedEntryMemoryCost,
			stats.Expirations,
			stats.Evictions,
		)
		if nca.Size() != 0 {
			return false
		}
		// make assertions over near cache stats.
		if stats.OwnedEntryCount != 0 {
			return false
		}
		if stats.OwnedEntryMemoryCost != 0 {
			return false
		}
		if stats.Expirations != int64(size) {
			return false
		}
		if stats.Evictions != 0 {
			return false
		}
		return true
	})
}

// this is the same entry processor from the hazelcast_test package.
// TODO: move this to it
const simpleEntryProcessorFactoryID = 66
const simpleEntryProcessorClassID = 1

type SimpleEntryProcessor struct {
	value string
}

func (s SimpleEntryProcessor) FactoryID() int32 {
	return simpleEntryProcessorFactoryID
}

func (s SimpleEntryProcessor) ClassID() int32 {
	return simpleEntryProcessorClassID
}

func (s SimpleEntryProcessor) WriteData(output serialization.DataOutput) {
	output.WriteString(s.value)
}

func (s *SimpleEntryProcessor) ReadData(input serialization.DataInput) {
	s.value = input.ReadString()
}

func expirationBefore(tcx it.MapTestContext) {
	t := tcx.T
	if err := os.Setenv(inearcache.EnvExpirationTaskInitialDelay, "0s"); err != nil {
		t.Logf("WARNING, could not set: %s", inearcache.EnvExpirationTaskInitialDelay)
	}
	if err := os.Setenv(inearcache.EnvExpirationTaskPeriod, "1s"); err != nil {
		t.Logf("WARNING, could not set: %s", inearcache.EnvExpirationTaskPeriod)
	}
}

func expirationAfter(tcx it.MapTestContext) {
	// ignoring the error
	_ = os.Setenv(inearcache.EnvExpirationTaskInitialDelay, "")
	// ignoring the error
	_ = os.Setenv(inearcache.EnvExpirationTaskPeriod, "")
}
