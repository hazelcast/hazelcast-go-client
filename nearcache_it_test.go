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

package hazelcast_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
)

const (
	nearCacheDefaultRecordCount = 1000
	defaultNearCacheName        = "defaultNearCache"
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
		//{inMemoryFmt: nearcache.InMemoryFormatBinary, serializeKeys: false},
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
			configCB := func(cfg *hz.Config) {
				cfg.AddNearCacheConfig(ncc)
			}
			valueFmt := "value-%d"
			mtcx := &it.MapTestContext{
				T:              t,
				ConfigCallback: configCB,
			}
			it.MapTesterWithContext(mtcx, func(mtcx *it.MapTestContext) {
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
				tcx.AssertNearCacheSize(nearCacheDefaultRecordCount)
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

func TestGetNearCache(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testGetAsync
	// this test is in this file, since it requires hazelcastinternaltest tag.
	testName := t.Name()
	makeName := func(p ...string) string {
		p = append([]string{testName}, p...)
		return strings.Join(p, "-")
	}
	configCB := func(cfg *hz.Config) {
		ncc := nearcache.Config{
			Name:           "*",
			InMemoryFormat: nearcache.InMemoryFormatObject,
		}
		ncc.SetInvalidateOnChange(false)
		cfg.AddNearCacheConfig(ncc)
	}
	it.MapTesterWithConfigAndName(t, makeName, configCB, func(t *testing.T, m *hz.Map) {
		const size = 1009
		ctx := context.Background()
		// populate map
		for i := 0; i < size; i++ {
			it.MustValue(m.Put(ctx, i, i))
		}
		// populate near cache
		for i := 0; i < size; i++ {
			v := it.MustValue(m.Get(ctx, i))
			if !assert.Equal(t, int64(i), v) {
				t.FailNow()
			}
		}
		// generate near cache hits
		for i := 0; i < size; i++ {
			v := it.MustValue(m.Get(ctx, i))
			if !assert.Equal(t, int64(i), v) {
				t.FailNow()
			}
		}
		stats := m.LocalMapStats().NearCacheStats
		assert.Equal(t, int64(size), stats.Hits)
		assert.Equal(t, int64(size), stats.OwnedEntryCount)
	})
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
