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
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/internal/it/skip"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestAtomicLong(t *testing.T) {
	skip.If(t, "enterprise")
	testCases := []struct {
		name       string
		f          func(t *testing.T)
		noParallel bool
	}{
		{name: "AtomicLongAddAndGet", f: atomicLongAddAndGetTest},
		{name: "AtomicLongAlter", f: atomicLongAlterTest},
		{name: "AtomicLongAlterAndGet", f: atomicLongAlterAndGetTest},
		{name: "AtomicLongApply", f: atomicLongApplyTest},
		{name: "AtomicLongCompareAndSet_Fail", f: atomicLongCompareAndSetFailTest},
		{name: "AtomicLongCompareAndSet_Success", f: atomicLongCompareAndSetSuccessTest},
		{name: "AtomicLongDecrementAndGet", f: atomicLongDecrementAndGetTest},
		{name: "AtomicLongGet", f: atomicLongGetTest},
		{name: "AtomicLongGetAndAdd", f: atomicLongGetAndAddTest},
		{name: "AtomicLongGetAndAlter", f: atomicLongGetAndAlterTest},
		{name: "AtomicLongGetAndDecrement", f: atomicLongGetAndDecrementTest},
		{name: "AtomicLongGetAndIncrement", f: atomicLongGetAndIncrementTest},
		{name: "AtomicLongGetAndSet", f: atomicLongGetAndSetTest},
		{name: "AtomicLongIncrementAndGet", f: atomicLongIncrementAndGetTest},
		{name: "AtomicLongSet", f: atomicLongSetTest},
	}
	// run no-parallel test first
	sort.Slice(testCases, func(i, j int) bool {
		return testCases[i].noParallel && !testCases[j].noParallel
	})
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if !tc.noParallel {
				t.Parallel()
			}
			tc.f(t)
		})
	}
}

func atomicLongSetTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testSet
	it.AtomicLongTester(t, func(t *testing.T, a *hz.AtomicLong) {
		err := a.Set(context.Background(), 271)
		require.NoError(t, err)
		v, err := a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(271), v)
	})
}

func atomicLongGetTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testGet
	it.AtomicLongTester(t, func(t *testing.T, a *hz.AtomicLong) {
		v, err := a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(0), v)
	})
}

func atomicLongAddAndGetTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testAddAndGet
	it.AtomicLongTester(t, func(t *testing.T, a *hz.AtomicLong) {
		v, err := a.AddAndGet(context.Background(), 271)
		require.NoError(t, err)
		require.Equal(t, v, int64(271))
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(271), v)
	})
}

func atomicLongCompareAndSetSuccessTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testCompareAndSet_whenSuccess
	it.AtomicLongTester(t, func(t *testing.T, a *hz.AtomicLong) {
		v, err := a.CompareAndSet(context.Background(), 0, 271)
		require.NoError(t, err)
		require.True(t, v)
		v1, err := a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v1, int64(271))
	})
}

func atomicLongCompareAndSetFailTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testCompareAndSet_whenNotSuccess
	it.AtomicLongTester(t, func(t *testing.T, a *hz.AtomicLong) {
		v, err := a.CompareAndSet(context.Background(), 172, 0)
		require.NoError(t, err)
		require.False(t, v)
		v1, err := a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v1, int64(0))
	})
}

func atomicLongDecrementAndGetTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testDecrementAndGet
	it.AtomicLongTester(t, func(t *testing.T, a *hz.AtomicLong) {
		v, err := a.DecrementAndGet(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(-1))
		v, err = a.DecrementAndGet(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(-2))
	})
}

func atomicLongGetAndSetTest(t *testing.T) {
	// ported from com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testGetAndSet
	it.AtomicLongTester(t, func(t *testing.T, a *hz.AtomicLong) {
		v, err := a.GetAndSet(context.Background(), 271)
		require.NoError(t, err)
		require.Equal(t, v, int64(0))
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(271))
	})
}

func atomicLongGetAndIncrementTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testGetAndIncrement
	it.AtomicLongTester(t, func(t *testing.T, a *hz.AtomicLong) {
		v, err := a.GetAndIncrement(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(0))
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(1))
	})
}

func atomicLongIncrementAndGetTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testIncrementAndGet
	it.AtomicLongTester(t, func(t *testing.T, a *hz.AtomicLong) {
		v, err := a.IncrementAndGet(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(1))
		v, err = a.IncrementAndGet(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(2))
	})
}

func atomicLongGetAndAddTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testGetAndAdd
	it.AtomicLongTester(t, func(t *testing.T, a *hz.AtomicLong) {
		v, err := a.GetAndAdd(context.Background(), 271)
		require.NoError(t, err)
		require.Equal(t, v, int64(0))
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(271))
	})
}

func atomicLongGetAndDecrementTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testGetAndDecrement
	it.AtomicLongTester(t, func(t *testing.T, a *hz.AtomicLong) {
		v, err := a.GetAndDecrement(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(0))
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(-1))
	})
}

func atomicLongApplyTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testApply
	cb := func(c *hz.Config) {
		c.Serialization.SetIdentifiedDataSerializableFactories(&MultiplicationFactory{})
	}
	it.AtomicLongTesterWithConfig(t, cb, func(t *testing.T, a *hz.AtomicLong) {
		err := a.Set(context.Background(), 2)
		require.NoError(t, err)
		v, err := a.Apply(context.Background(), &Multiplication{2})
		require.NoError(t, err)
		require.Equal(t, v, int64(4))
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(2))
	})
}

func atomicLongAlterTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testAlter
	cb := func(c *hz.Config) {
		c.Serialization.SetIdentifiedDataSerializableFactories(&MultiplicationFactory{})
	}
	it.AtomicLongTesterWithConfig(t, cb, func(t *testing.T, a *hz.AtomicLong) {
		err := a.Set(context.Background(), 2)
		require.NoError(t, err)
		err = a.Alter(context.Background(), &Multiplication{2})
		require.NoError(t, err)
		v, err := a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(4))

	})
}

func atomicLongGetAndAlterTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testGetAndAlter
	cb := func(c *hz.Config) {
		c.Serialization.SetIdentifiedDataSerializableFactories(&MultiplicationFactory{})
	}
	it.AtomicLongTesterWithConfig(t, cb, func(t *testing.T, a *hz.AtomicLong) {
		err := a.Set(context.Background(), 2)
		require.NoError(t, err)
		v, err := a.GetAndAlter(context.Background(), &Multiplication{2})
		require.NoError(t, err)
		require.Equal(t, v, int64(2))
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(4))
	})
}

func atomicLongAlterAndGetTest(t *testing.T) {
	// ported from: com.hazelcast.cp.internal.datastructures.atomiclong.AbstractAtomicLongBasicTest#testAlterAndGet
	cb := func(c *hz.Config) {
		c.Serialization.SetIdentifiedDataSerializableFactories(&MultiplicationFactory{})
	}
	it.AtomicLongTesterWithConfig(t, cb, func(t *testing.T, a *hz.AtomicLong) {
		err := a.Set(context.Background(), 2)
		require.NoError(t, err)
		v, err := a.AlterAndGet(context.Background(), &Multiplication{2})
		require.NoError(t, err)
		require.Equal(t, v, int64(4))
	})
}

const multiplicationFactoryID = 66
const multiplicationProcessorClassID = 16

type Multiplication struct {
	multiplier int64
}

func (s Multiplication) FactoryID() int32 {
	return multiplicationFactoryID
}

func (s Multiplication) ClassID() int32 {
	return multiplicationProcessorClassID
}

func (s Multiplication) WriteData(output serialization.DataOutput) {
	output.WriteInt64(s.multiplier)
}

func (s *Multiplication) ReadData(input serialization.DataInput) {
	s.multiplier = input.ReadInt64()
}

type MultiplicationFactory struct {
}

func (f MultiplicationFactory) Create(id int32) serialization.IdentifiedDataSerializable {
	if id == multiplicationProcessorClassID {
		return &Multiplication{}
	}
	panic(fmt.Sprintf("unknown class ID: %d", id))
}

func (f MultiplicationFactory) FactoryID() int32 {
	return multiplicationFactoryID
}
