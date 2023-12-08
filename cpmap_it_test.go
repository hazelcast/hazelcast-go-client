/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/internal/check"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/internal/it/skip"
)

func TestCPMap(t *testing.T) {
	skip.IfNot(t, "hz >= 5.4, enterprise")
	testCases := []struct {
		name string
		f    func(t *testing.T)
	}{
		{name: "cpMapCompareAndSet", f: cpMapCompareAndSetTest},
		{name: "cpMapCompareAndSet_NilKeyValue", f: cpMapCompareAndSet_NilKeyValueTest},
		{name: "cpMapDelete", f: cpMapDeleteTest},
		{name: "cpMapDelete_NilKey", f: cpMapDelete_NilKeyTest},
		{name: "cpMapGet", f: cpMapGetTest},
		{name: "cpMapGet_NilKey", f: cpMapGet_NilKeyTest},
		{name: "cpMapPut", f: cpMapPutTest},
		{name: "cpMapPut_NilKeyValue", f: cpMapPut_NilKeyValueTest},
		{name: "cpMapRemove", f: cpMapRemoveTest},
		{name: "cpMapRemove_NilKey", f: cpMapRemove_NilKeyTest},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.f)
	}
}

func cpMapGetTest(t *testing.T) {
	tcx := it.CPMapTestContext{T: t}
	tcx.Tester(func(tcx *it.CPMapTestContext) {
		ctx := context.Background()
		v := check.MustValue(tcx.M.Get(ctx, "key"))
		require.Nil(t, v)
		check.Must(tcx.M.Set(ctx, "key", "value"))
		v = check.MustValue(tcx.M.Get(ctx, "key"))
		require.Equal(t, "value", v)
	})
}

func cpMapGet_NilKeyTest(t *testing.T) {
	tcx := it.CPMapTestContext{T: t}
	tcx.Tester(func(tcx *it.CPMapTestContext) {
		ctx := context.Background()
		// nil key should return an error
		_, err := tcx.M.Get(ctx, nil)
		require.Error(t, err)
	})
}

func cpMapPutTest(t *testing.T) {
	tcx := it.CPMapTestContext{T: t}
	tcx.Tester(func(tcx *it.CPMapTestContext) {
		ctx := context.Background()
		old := check.MustValue(tcx.M.Put(ctx, "key", "value"))
		require.Nil(t, old)
		v := check.MustValue(tcx.M.Get(ctx, "key"))
		require.Equal(t, "value", v)
	})
}

func cpMapPut_NilKeyValueTest(t *testing.T) {
	tcx := it.CPMapTestContext{T: t}
	tcx.Tester(func(tcx *it.CPMapTestContext) {
		ctx := context.Background()
		// nil key should return an error
		_, err := tcx.M.Put(ctx, nil, "value")
		require.Error(t, err)
		// nil value should return an error
		_, err = tcx.M.Put(ctx, "foo", nil)
		require.Error(t, err)
	})
}

func cpMapRemoveTest(t *testing.T) {
	tcx := it.CPMapTestContext{T: t}
	tcx.Tester(func(tcx *it.CPMapTestContext) {
		ctx := context.Background()
		v := check.MustValue(tcx.M.Remove(ctx, "key"))
		require.Nil(t, v)
		check.Must(tcx.M.Set(ctx, "key", "value"))
		v = check.MustValue(tcx.M.Remove(ctx, "key"))
		require.Equal(t, "value", v)
	})
}

func cpMapRemove_NilKeyTest(t *testing.T) {
	tcx := it.CPMapTestContext{T: t}
	tcx.Tester(func(tcx *it.CPMapTestContext) {
		ctx := context.Background()
		// nil key should return an error
		_, err := tcx.M.Remove(ctx, nil)
		require.Error(t, err)
	})
}

func cpMapDeleteTest(t *testing.T) {
	tcx := it.CPMapTestContext{T: t}
	tcx.Tester(func(tcx *it.CPMapTestContext) {
		ctx := context.Background()
		check.Must(tcx.M.Set(ctx, "key", "value"))
		v := check.MustValue(tcx.M.Get(ctx, "key"))
		require.Equal(t, "value", v)
		check.Must(tcx.M.Delete(ctx, "key"))
		v = check.MustValue(tcx.M.Get(ctx, "key"))
		require.Equal(t, nil, v)
	})
}

func cpMapDelete_NilKeyTest(t *testing.T) {
	tcx := it.CPMapTestContext{T: t}
	tcx.Tester(func(tcx *it.CPMapTestContext) {
		ctx := context.Background()
		// nil key should return an error
		err := tcx.M.Delete(ctx, nil)
		require.Error(t, err)
	})
}

func cpMapCompareAndSetTest(t *testing.T) {
	tcx := it.CPMapTestContext{T: t}
	tcx.Tester(func(tcx *it.CPMapTestContext) {
		ctx := context.Background()
		ok := check.MustValue(tcx.M.CompareAndSet(ctx, "key", "foo", "bar"))
		require.False(t, ok)
		check.Must(tcx.M.Set(ctx, "key", "foo"))
		ok = check.MustValue(tcx.M.CompareAndSet(ctx, "key", "foo", "bar"))
		require.True(t, ok)
	})
}

func cpMapCompareAndSet_NilKeyValueTest(t *testing.T) {
	tcx := it.CPMapTestContext{T: t}
	tcx.Tester(func(tcx *it.CPMapTestContext) {
		ctx := context.Background()
		// nil key should return an error
		_, err := tcx.M.CompareAndSet(ctx, nil, "a", "b")
		require.Error(t, err)
		// nil expected value should return an error
		_, err = tcx.M.CompareAndSet(ctx, "foo", nil, "b")
		require.Error(t, err)
		// nil new value should return an error
		_, err = tcx.M.CompareAndSet(ctx, "foo", "a", nil)
		require.Error(t, err)
	})
}
