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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestPNCounter_AddAndGet(t *testing.T) {
	it.PNCounterTester(t, func(t *testing.T, pn *hz.PNCounter) {
		v, err := pn.AddAndGet(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(1), v)
		v, err = pn.AddAndGet(context.Background(), 10)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(11), v)
	})
}

func TestPNCounter_Get(t *testing.T) {
	it.PNCounterTester(t, func(t *testing.T, pn *hz.PNCounter) {
		v, err := pn.Get(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(0), v)
	})
}

func TestPNCounter_GetAndAdd(t *testing.T) {
	it.PNCounterTester(t, func(t *testing.T, pn *hz.PNCounter) {
		v, err := pn.GetAndAdd(context.Background(), 5)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(0), v)
		v, err = pn.GetAndAdd(context.Background(), 10)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(5), v)
	})
}

func TestPNCounter_GetAndDecrement(t *testing.T) {
	it.PNCounterTester(t, func(t *testing.T, pn *hz.PNCounter) {
		v, err := pn.GetAndDecrement(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(0), v)
		v, err = pn.GetAndDecrement(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(-1), v)
	})
}

func TestPNCounter_GetAndIncrement(t *testing.T) {
	it.PNCounterTester(t, func(t *testing.T, pn *hz.PNCounter) {
		v, err := pn.GetAndIncrement(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(0), v)
		v, err = pn.GetAndIncrement(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(1), v)
	})
}

func TestPNCounter_GetAndSubtract(t *testing.T) {
	it.PNCounterTester(t, func(t *testing.T, pn *hz.PNCounter) {
		v, err := pn.GetAndSubtract(context.Background(), 5)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(0), v)
		v, err = pn.GetAndSubtract(context.Background(), 10)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(-5), v)
	})
}

func TestPNCounter_DecrementAndGet(t *testing.T) {
	it.PNCounterTester(t, func(t *testing.T, pn *hz.PNCounter) {
		v, err := pn.DecrementAndGet(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(-1), v)
		v, err = pn.DecrementAndGet(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(-2), v)
	})
}

func TestPNCounter_IncrementAndGet(t *testing.T) {
	it.PNCounterTester(t, func(t *testing.T, pn *hz.PNCounter) {
		v, err := pn.IncrementAndGet(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(1), v)
		v, err = pn.IncrementAndGet(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(2), v)
	})
}

func TestPNCounter_SubtractAndGet(t *testing.T) {
	it.PNCounterTester(t, func(t *testing.T, pn *hz.PNCounter) {
		v, err := pn.SubtractAndGet(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(-1), v)
		v, err = pn.SubtractAndGet(context.Background(), 10)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(-11), v)
	})
}

func TestPNCounter_Reset(t *testing.T) {
	it.PNCounterTester(t, func(t *testing.T, pn *hz.PNCounter) {
		pn.Reset()
	})
}

func TestPNCounter_Add1000Sub1000(t *testing.T) {
	it.PNCounterTester(t, func(t *testing.T, pn *hz.PNCounter) {
		const count = 1000
		wg := &sync.WaitGroup{}
		wg.Add(count * 2)
		ctx := context.Background()
		for i := 0; i < count; i++ {
			go func() {
				if _, err := pn.IncrementAndGet(ctx); err != nil {
					panic(err)
				}
				wg.Done()
			}()
		}
		for i := 0; i < count; i++ {
			go func() {
				if _, err := pn.DecrementAndGet(ctx); err != nil {
					panic(err)
				}
				wg.Done()
			}()
		}
		wg.Wait()
		v, err := pn.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(0), v)
	})
}
