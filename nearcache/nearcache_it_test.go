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
	"time"

	"github.com/stretchr/testify/require"

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
		const size = int64(1009)
		ctx := context.Background()
		populateMap(tcx, size)
		populateNearCache(tcx, size)
		// generate near cache hits
		for i := int64(0); i < size; i++ {
			v := it.MustValue(m.Get(ctx, i))
			require.Equal(t, i, v)
		}
		stats := m.LocalMapStats().NearCacheStats
		require.Equal(t, size, stats.Hits)
		require.Equal(t, size, stats.OwnedEntryCount)
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
				require.Equal(tcx.T, v, i)
			},
		},
		{
			name: "RemoveIfSame",
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
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
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
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
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
				v, err := tcx.M.Put(ctx, i, i)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.Equal(t, i, v)
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
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
				if err := tcx.M.Set(ctx, i, i); err != nil {
					tcx.T.Fatal(err)
				}
			},
		},
		{
			name: "SetWithTTL",
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
				if err := tcx.M.SetWithTTL(ctx, i, i, 1*time.Second); err != nil {
					tcx.T.Fatal(err)
				}
			},
		},
		{
			name: "SetWithTTLAndMaxIdle",
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
				if err := tcx.M.SetWithTTLAndMaxIdle(ctx, i, i, 1*time.Second, 2*time.Second); err != nil {
					tcx.T.Fatal(err)
				}
			},
		},
	}
	invalidationRunner(t, testCases)
}

func TestAfterTryRemoveNearCacheIsInvalidated(t *testing.T) {
	// port of: com.hazelcast.client.map.impl.nearcache.ClientMapNearCacheTest#testAfterTryRemoveNearCacheIsInvalidated
	testCases := []mapTestCase{
		{
			name: "TryRemove",
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
				v, err := tcx.M.TryRemove(ctx, i)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.True(t, v.(bool))
			},
		},
		{
			name: "TryRemoveWithTimeout",
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
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
			name: "Put",
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
				v, err := tcx.M.Put(ctx, i, i)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.Equal(tcx.T, i, v)
			},
		},
		{
			name: "PutWithMaxIdle",
			f: func(ctx context.Context, tcx it.MapTestContext, i int64) {
				v, err := tcx.M.PutWithMaxIdle(ctx, i, i, 10*time.Minute)
				if err != nil {
					tcx.T.Fatal(err)
				}
				require.Equal(tcx.T, i, v)
			},
		},
	}
	invalidationRunner(t, testCases)
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
				nca := hz.MakeNearCacheAdapterFromMap(mtcx.M)
				ci := hz.NewClientInternal(mtcx.Client)
				tcx := it.NewNearCacheTestContext(mtcx.T, nca.(it.NearCacheAdapter), mtcx.M, &ncc, ci.SerializationService())
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
				tcx.AssertNearCacheContent(nearCacheDefaultRecordCount, valueFmt)
				// TODO: assertNearCacheReferences
			})
		})
	}
}

func invalidationRunner(t *testing.T, testCases []mapTestCase) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatBinary, true)
			tcx.Tester(func(tcx it.MapTestContext) {
				const size = int64(1000)
				ctx := context.Background()
				populateMap(tcx, size)
				populateNearCache(tcx, size)
				require.Equal(t, size, tcx.M.LocalMapStats().NearCacheStats.OwnedEntryCount)
				for i := int64(0); i < size; i++ {
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
