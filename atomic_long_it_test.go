package hazelcast_test

import (
	"context"
	"fmt"
	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cp"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/stretchr/testify/require"
	"sort"
	"testing"
)

func TestAtomicLong(t *testing.T) {
	testCases := []struct {
		name       string
		f          func(t *testing.T)
		noParallel bool
	}{
		{name: "AtomicLongSet", f: atomicLongSet},
		{name: "AtomicLongGet", f: atomicLongGet},
		{name: "AtomicLongAddAndGet", f: atomicLongAddAndGet},
		{name: "AtomicLongCompareAndSet_Success", f: atomicLongCompareAndSet},
		{name: "AtomicLongCompareAndSet_Fail", f: atomicLongCompareAndSetFail},
		{name: "AtomicLongDecrementAndGet", f: atomicLongDecrementAndGet},
		{name: "AtomicLongGetAndSet", f: atomicLongGetAndSet},
		{name: "AtomicLongGetAndIncrement", f: atomicLongGetAndIncrement},
		{name: "AtomicLongIncrementAndGet", f: atomicLongIncrementAndGet},
		{name: "AtomicLongGetAndAdd", f: atomicLongGetAndAdd},
		{name: "AtomicLongGetAndDecrement", f: atomicLongGetAndDecrement},
		{name: "AtomicLongApply", f: atomicLongApply},
		{name: "AtomicLongAlter", f: atomicLongAlter},
		{name: "AtomicLongGetAndAlter", f: atomicLongGetAndAlter},
		{name: "AtomicLongAlterAndGet", f: atomicLongAlterAndGet},
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

func atomicLongSet(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		err := a.Set(context.Background(), 271)
		require.NoError(t, err)
		v, err := a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(271), v)
	})
}

func atomicLongGet(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		v, err := a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(0), v)
	})
}

func atomicLongAddAndGet(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		v, err := a.AddAndGet(context.Background(), 271)
		require.NoError(t, err)
		require.Equal(t, v, int64(271))
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, int64(271), v)
	})
}

func atomicLongCompareAndSet(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		v, err := a.CompareAndSet(context.Background(), 0, 271)
		require.NoError(t, err)
		require.True(t, v)
		v1, err := a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v1, int64(271))
	})
}

func atomicLongCompareAndSetFail(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		v, err := a.CompareAndSet(context.Background(), 172, 0)
		require.NoError(t, err)
		require.False(t, v)
		v1, err := a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v1, int64(0))
	})
}

func atomicLongDecrementAndGet(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		v, err := a.DecrementAndGet(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(-1))
		v, err = a.DecrementAndGet(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(-2))
	})
}

func atomicLongGetAndSet(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		v, err := a.GetAndSet(context.Background(), 271)
		require.NoError(t, err)
		require.Equal(t, v, int64(0))
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(271))
	})
}

func atomicLongGetAndIncrement(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		v, err := a.GetAndIncrement(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(0))
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(1))
	})
}

func atomicLongIncrementAndGet(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		v, err := a.IncrementAndGet(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(1))
		v, err = a.IncrementAndGet(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(2))
	})
}

func atomicLongGetAndAdd(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		v, err := a.GetAndAdd(context.Background(), 271)
		require.NoError(t, err)
		require.Equal(t, v, int64(0))
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(271))
	})
}

func atomicLongGetAndDecrement(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		v, err := a.GetAndDecrement(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(0))
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(-1))
	})
}

func atomicLongApply(t *testing.T) {
	cb := func(c *hz.Config) {
		c.Serialization.SetIdentifiedDataSerializableFactories(&MultiplicationFactory{})
	}
	it.AtomicLongTesterWithConfig(t, cb, func(t *testing.T, a cp.AtomicLong) {
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

func atomicLongAlter(t *testing.T) {
	cb := func(c *hz.Config) {
		c.Serialization.SetIdentifiedDataSerializableFactories(&MultiplicationFactory{})
	}
	it.AtomicLongTesterWithConfig(t, cb, func(t *testing.T, a cp.AtomicLong) {
		err := a.Set(context.Background(), 2)
		require.NoError(t, err)
		err = a.Alter(context.Background(), &Multiplication{2})
		require.NoError(t, err)
		v, err := a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, v, int64(4))

	})
}

func atomicLongGetAndAlter(t *testing.T) {
	cb := func(c *hz.Config) {
		c.Serialization.SetIdentifiedDataSerializableFactories(&MultiplicationFactory{})
	}
	it.AtomicLongTesterWithConfig(t, cb, func(t *testing.T, a cp.AtomicLong) {
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

func atomicLongAlterAndGet(t *testing.T) {
	cb := func(c *hz.Config) {
		c.Serialization.SetIdentifiedDataSerializableFactories(&MultiplicationFactory{})
	}
	it.AtomicLongTesterWithConfig(t, cb, func(t *testing.T, a cp.AtomicLong) {
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
