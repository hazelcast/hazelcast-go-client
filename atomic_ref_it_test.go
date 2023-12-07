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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/internal/check"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestAtomicRef(t *testing.T) {
	testCases := []struct {
		name string
		f    func(t *testing.T)
	}{
		{name: "atomicRefClear", f: atomicRefClearTest},
		{name: "atomicRefCompareAndSet", f: atomicRefCompareAndSetTest},
		{name: "atomicRefContains", f: atomicRefContainsTest},
		{name: "atomicRefGet", f: atomicRefGetTest},
		{name: "atomicRefGetAndSet", f: atomicRefGetAndSetTest},
		{name: "atomicRefIsNil", f: atomicRefIsNilTest},
		{name: "atomicRefName", f: atomicRefNametest},
	}
	for _, tc := range testCases {
		t.Run(tc.name, tc.f)
	}
}

func atomicRefCompareAndSetTest(t *testing.T) {
	tcx := it.AtomicRefTestContext{T: t}
	tcx.Tester(func(tcx *it.AtomicRefTestContext) {
		ctx := context.Background()
		ok := check.MustValue(tcx.A.CompareAndSet(ctx, nil, "bar"))
		require.True(t, ok)
		ok = check.MustValue(tcx.A.CompareAndSet(ctx, "bar", "foo"))
		require.True(t, ok)
	})
}

func atomicRefGetTest(t *testing.T) {
	tcx := it.AtomicRefTestContext{T: t}
	tcx.Tester(func(tcx *it.AtomicRefTestContext) {
		ctx := context.Background()
		value := check.MustValue(tcx.A.Get(ctx))
		require.Equal(t, nil, value)
		check.Must(tcx.A.Set(ctx, "foo"))
		value = check.MustValue(tcx.A.Get(ctx))
		require.Equal(t, "foo", value)
	})
}

func atomicRefGetAndSetTest(t *testing.T) {
	tcx := it.AtomicRefTestContext{T: t}
	tcx.Tester(func(tcx *it.AtomicRefTestContext) {
		ctx := context.Background()
		v1 := check.MustValue(tcx.A.GetAndSet(ctx, "foo"))
		require.Equal(t, nil, v1)
		v2 := check.MustValue(tcx.A.GetAndSet(ctx, "bar"))
		require.Equal(t, "foo", v2)
	})
}

func atomicRefContainsTest(t *testing.T) {
	tcx := it.AtomicRefTestContext{T: t}
	tcx.Tester(func(tcx *it.AtomicRefTestContext) {
		ctx := context.Background()
		found := check.MustValue(tcx.A.Contains(ctx, "foo"))
		require.False(t, found)
		check.Must(tcx.A.Set(ctx, "foo"))
		found = check.MustValue(tcx.A.Contains(ctx, "foo"))
		require.True(t, found)
	})
}

func atomicRefIsNilTest(t *testing.T) {
	tcx := it.AtomicRefTestContext{T: t}
	tcx.Tester(func(tcx *it.AtomicRefTestContext) {
		ctx := context.Background()
		ok := check.MustValue(tcx.A.IsNil(ctx))
		require.True(t, ok)
		check.Must(tcx.A.Set(ctx, "foo"))
		ok = check.MustValue(tcx.A.IsNil(ctx))
		require.False(t, ok)
	})
}

func atomicRefClearTest(t *testing.T) {
	tcx := it.AtomicRefTestContext{T: t}
	tcx.Tester(func(tcx *it.AtomicRefTestContext) {
		ctx := context.Background()
		check.Must(tcx.A.Set(ctx, "foo"))
		v := check.MustValue(tcx.A.Get(ctx))
		require.Equal(t, "foo", v)
		check.Must(tcx.A.Clear(ctx))
		v = check.MustValue(tcx.A.Get(ctx))
		require.Equal(t, nil, v)
	})
}

func atomicRefNametest(t *testing.T) {
	name := it.NewUniqueObjectName("atomicref")
	nameWithGroup := name + "@somegroup"
	tcx := it.AtomicRefTestContext{
		T:    t,
		Name: nameWithGroup,
	}
	tcx.Tester(func(tcx *it.AtomicRefTestContext) {
		assert.Equal(t, tcx.A.Name(), name)
	})
}
