//go:build hazelcastinternal && hazelcastinternaltest
// +build hazelcastinternal,hazelcastinternaltest

/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
	"testing"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

const (
	nearCacheDefaultRecordCount = 1000
)

// TestWhenGetIsUsedThenNearCacheShouldBePopulated checks that the Near Cache is populated when Get is used.
// And also the NearCacheStats are calculated correctly.
func TestWhenGetIsUsedThenNearCacheShouldBePopulated(t *testing.T) {
	// ported from: com.hazelcast.internal.nearcache.impl.AbstractNearCacheBasicTest#whenGetIsUsed_thenNearCacheShouldBePopulated()
	ClientCacheNearCacheBasicSlowRunner(t, func(tcx *it.NearCacheTestContext, size int64, valueFmt string) {
		tcx.PopulateNearCacheWithGet(size, valueFmt)
	})
}

func ClientCacheNearCacheBasicSlowRunner(t *testing.T, f func(tcx *it.NearCacheTestContext, size int64, valueFmt string)) {
	testCases := []struct {
		inMemoryFmt   nearcache.InMemoryFormat
		serializeKeys bool
	}{
		//{inMemoryFmt: nearcache.InMemoryFormatBinary, serializeKeys: true},
		{inMemoryFmt: nearcache.InMemoryFormatBinary, serializeKeys: false},
		//{inMemoryFmt: nearcache.InMemoryFormatObject, serializeKeys: true},
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
				tcx.Config.AddNearCacheConfig(ncc)
			}
			valueFmt := "value-%d"
			mtcx := &it.MapTestContext{
				T:              t,
				ConfigCallback: configCB,
			}
			mtcx.Tester(func(mtcx it.MapTestContext) {
				nca := hz.MakeNearCacheAdapterFromMap(mtcx.M)
				dsa := it.NewMapDataStructureAdapter(mtcx.M)
				ci := hz.NewClientInternal(mtcx.Client)
				tcx := it.NewNearCacheTestContext(mtcx.T, nca.(it.NearCacheAdapter), dsa, &ncc, ci.SerializationService())
				// assert that the Near Cache is empty
				tcx.PopulateNearCacheDataAdapter(nearCacheDefaultRecordCount, valueFmt)
				tcx.AssertNearCacheSize(0)
				tcx.AssertNearCacheStats(nearcache.Stats{
					OwnedEntryCount: 0,
					Hits:            0,
					Misses:          0,
				})
				// populate the Near Cache
				f(tcx, nearCacheDefaultRecordCount, valueFmt)
				mtcx.OK(tcx.AssertNearCacheSize(nearCacheDefaultRecordCount))
				tcx.AssertNearCacheStats(nearcache.Stats{
					OwnedEntryCount: nearCacheDefaultRecordCount,
					Hits:            0,
					Misses:          nearCacheDefaultRecordCount,
				})
				// generate Near Cache hits
				f(tcx, nearCacheDefaultRecordCount, valueFmt)
				tcx.AssertNearCacheSize(nearCacheDefaultRecordCount)
				tcx.AssertNearCacheStats(nearcache.Stats{
					OwnedEntryCount: nearCacheDefaultRecordCount,
					Hits:            nearCacheDefaultRecordCount,
					Misses:          nearCacheDefaultRecordCount,
				})
				tcx.AssertNearCacheContent(nearCacheDefaultRecordCount, valueFmt)
				// TODO: assertNearCacheReferences
			})
		})
	}
}

/*
func TestGetAllChecksNearCacheFirst(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testGetAllChecksNearCacheFirst
	var mapName string
	makeName := func(p ...string) string {
		p = append([]string{"nearcache"}, p...)
		mapName = strings.Join(p, "-")
		return mapName
	}
	configCB := func(cfg *hz.Config) {
		ncc := nearcache.Config{Name: "nearcache*"}
		cfg.AddNearCacheConfig(ncc)
	}
	it.MapTesterWithConfigAndName(t, makeName, configCB, func(t *testing.T, m *hz.Map) {
		const size = 1003
		ctx := context.Background()
		var keys []interface{}
		for i := 0; i < size; i++ {
			it.MustValue(m.Put(ctx, i, i))
			keys = append(keys, i)
		}
		// populate near cache
		for i := 0; i < size; i++ {
			it.MustValue(m.Get(ctx, i))
		}
		// GetAll generates the near cache hits
		it.MustValue(m.GetAll(ctx, keys))
		stats := m.localMapStats().NearCacheStats
		assert.Equal(t, size, stats.Hits)
		//assert.Equal(t, size, stats.OwnedEntryCount)
	})
}
*/

func TestNearCacheGet(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testGetAsync
	tcx := it.MapTestContext{
		T: t,
		ConfigCallback: func(tcx it.MapTestContext) {
			ncc := nearcache.Config{
				Name:           tcx.MapName,
				InMemoryFormat: nearcache.InMemoryFormatObject,
			}
			ncc.SetInvalidateOnChange(false)
			tcx.Config.AddNearCacheConfig(ncc)
		},
	}
	tcx.Tester(func(tcx it.MapTestContext) {
		m := tcx.M
		t := tcx.T
		const size = int64(1009)
		ctx := context.Background()
		populateMap(tcx, size)
		populateNearCache(tcx, size)
		// generate near cache hits
		for i := int64(0); i < size; i++ {
			v := it.MustValue(m.Get(ctx, i))
			tcx.OK(assert.Equal(t, i, v))
		}
		stats := m.LocalMapStats().NearCacheStats
		tcx.OK(assert.Equal(t, size, stats.Hits))
		tcx.OK(assert.Equal(t, size, stats.OwnedEntryCount))
	})
}

type mapTestCase struct {
	name string
	f    func(context.Context, it.MapTestContext, int64)
}

func TestAfterRemoveNearCacheIsInvalidated(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterRemoveNearCacheIsInvalidated
	testCases := []mapTestCase{
		{
			name: "Remove",
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
				v, err := tcx.M.Remove(ctx, i)
				if err != nil {
					tcx.T.Fatal(err)
				}
				tcx.OK(assert.Equal(tcx.T, v, i))
			},
		},
		{
			name: "RemoveIfSame",
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
				b, err := tcx.M.RemoveIfSame(ctx, i, i)
				if err != nil {
					tcx.T.Fatal(err)
				}
				tcx.OK(assert.True(tcx.T, b))
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
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
				if err := tcx.M.Delete(ctx, i); err != nil {
					tcx.T.Fatal(err)
				}
			},
		},
	}
	invalidationRunner(t, testCases)
}

func invalidationRunner(t *testing.T, testCases []mapTestCase) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tcx := it.MapTestContext{
				T: t,
				ConfigCallback: func(tcx it.MapTestContext) {
					ncc := nearcache.Config{
						Name: tcx.MapName,
					}
					ncc.SetInvalidateOnChange(false)
					tcx.Config.AddNearCacheConfig(ncc)
				},
			}
			tcx.Tester(func(tcx it.MapTestContext) {
				const size = int64(1000)
				ctx := context.Background()
				populateMap(tcx, size)
				populateNearCache(tcx, size)
				tcx.OK(assert.Equal(t, size, tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount))
				for i := int64(0); i < size; i++ {
					tc.f(ctx, tcx, i)
				}
				tcx.OK(assert.Equal(t, int64(0), tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount))
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

func populateMap(tcx it.MapTestContext, size int64) {
	for i := int64(0); i < size; i++ {
		if _, err := tcx.M.Put(context.Background(), i, i); err != nil {
			panic(err)
		}
	}
}

func populateNearCache(tcx it.MapTestContext, size int64) {
	for i := int64(0); i < size; i++ {
		v, err := tcx.M.Get(context.Background(), i)
		if err != nil {
			tcx.T.Fatal(err)
		}
		it.EnsureOK(tcx.T, assert.Equal(tcx.T, v, i))
	}
}
