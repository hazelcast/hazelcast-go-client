package cp

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client/cp"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
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
		{name: "AddIndexValidationError", f: TestAtomicLong},
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

func TestAtomicLong_Set(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		err := a.Set(context.Background(), 271)
		require.NoError(t, err)
		v, err := a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, 271, v)
	})
}

func TestAtomicLong_Get(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		v, err := a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, 0, v)
	})
}

func TestAtomicLong_AddAndGet(t *testing.T) {
	it.AtomicLongTester(t, func(t *testing.T, a cp.AtomicLong) {
		v, err := a.AddAndGet(context.Background(), 271)
		require.NoError(t, err)
		require.Equal(t, v, 271)
		v, err = a.Get(context.Background())
		require.NoError(t, err)
		require.Equal(t, 271, v)
	})
}

func TestAtomicLong_CompareAndSet(t *testing.T) {

}

func TestAtomicLong_DecrementAndGet(t *testing.T) {

}

func TestAtomicLong_GetAndSet(t *testing.T) {

}

func TestAtomicLong_GetAndIncrement(t *testing.T) {

}

func TestAtomicLong_IncrementAndGet(t *testing.T) {

}

func TestAtomicLong_GetAndAdd(t *testing.T) {

}

func TestAtomicLong_GetAndDecrement(t *testing.T) {

}

func TestAtomicLong_Apply(t *testing.T) {

}
