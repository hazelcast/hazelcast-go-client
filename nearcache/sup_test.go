/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/nearcache"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestGetAllAfterInvalidation(t *testing.T) {
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, false)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		ctx := context.Background()
		var keys []interface{}
		var target []types.Entry
		const size = 3
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
		// change just one of the values
		target[1] = types.Entry{
			Key:   keys[1],
			Value: "foo",
		}
		it.Must(m.Set(ctx, target[1].Key, target[1].Value))
		vs, err = m.GetAll(ctx, keys...)
		if err != nil {
			t.Fatal(err)
		}
		require.ElementsMatch(t, target, vs)
	})
}

func TestGetAllNonexistentEntries(t *testing.T) {
	tcx := newNearCacheMapTestContext(t, nearcache.InMemoryFormatObject, false)
	tcx.Tester(func(tcx it.MapTestContext) {
		t := tcx.T
		m := tcx.M
		ctx := context.Background()
		var keys []interface{}
		var target []types.Entry
		const size = 3
		for i := int64(0); i < size; i++ {
			keys = append(keys, i)
		}
		// no values
		vs, err := m.GetAll(ctx, keys...)
		if err != nil {
			t.Fatal(err)
		}
		require.ElementsMatch(t, target, vs)
		// single value
		it.Must(m.Set(ctx, keys[0], "value0"))
		vs, err = m.GetAll(ctx, keys...)
		if err != nil {
			t.Fatal(err)
		}
		target = []types.Entry{
			{Key: keys[0], Value: "value0"},
		}
		require.ElementsMatch(t, target, vs)
		// two values
		it.Must(m.Set(ctx, keys[2], "value2"))
		vs, err = m.GetAll(ctx, keys...)
		if err != nil {
			t.Fatal(err)
		}
		target = []types.Entry{
			{Key: keys[0], Value: "value0"},
			{Key: keys[2], Value: "value2"},
		}
		require.ElementsMatch(t, target, vs)
		// three values
		it.Must(m.Set(ctx, keys[1], "value1"))
		vs, err = m.GetAll(ctx, keys...)
		if err != nil {
			t.Fatal(err)
		}
		target = []types.Entry{
			{Key: keys[0], Value: "value0"},
			{Key: keys[1], Value: "value1"},
			{Key: keys[2], Value: "value2"},
		}
		require.ElementsMatch(t, target, vs)
	})
}
