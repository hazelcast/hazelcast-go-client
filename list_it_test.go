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
	"math"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestList_AddListener(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		const targetCallCount = int32(10)
		callCount := int32(0)
		subscriptionID, err := l.AddListener(context.Background(), false, func(event *hz.ListItemNotified) {
			if event.EventType == hz.ItemAdded {
				atomic.AddInt32(&callCount, 1)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(targetCallCount); i++ {
			item := fmt.Sprintf("item-%d", i)
			it.MustValue(l.Add(context.Background(), item))
		}
		it.Eventually(t, func() bool { return atomic.LoadInt32(&callCount) == targetCallCount })
		atomic.StoreInt32(&callCount, 0)
		if err = l.RemoveListener(context.Background(), subscriptionID); err != nil {
			t.Fatal(err)
		}
		it.MustValue(l.Add(context.Background(), "item-42"))
		it.Never(t, func() bool { return atomic.LoadInt32(&callCount) != 0 })
	})
}

func TestList_AddListener_IncludeValue(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		const targetCallCount = int32(10)
		callCount := int32(0)
		subscriptionID, err := l.AddListener(context.Background(), true, func(event *hz.ListItemNotified) {
			if event.EventType == hz.ItemRemoved {
				atomic.AddInt32(&callCount, 1)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(targetCallCount); i++ {
			item := fmt.Sprintf("item-%d", i)
			it.MustValue(l.Add(context.Background(), item))
			it.MustBool(l.Remove(context.Background(), item))
		}
		it.Eventually(t, func() bool { return atomic.LoadInt32(&callCount) == targetCallCount })
		atomic.StoreInt32(&callCount, 0)
		if err = l.RemoveListener(context.Background(), subscriptionID); err != nil {
			t.Fatal(err)
		}
		it.MustValue(l.Add(context.Background(), "item-42"))
		it.MustValue(l.Remove(context.Background(), "item-42"))
		it.Never(t, func() bool { return atomic.LoadInt32(&callCount) != 0 })
	})
}

func TestList_Add(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		changed, err := l.Add(context.Background(), "test1")
		assert.NoError(t, err)
		assert.True(t, changed)
		result, err := l.Get(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, "test1", result)
	})
}

func TestList_AddNilElement(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.Add(context.Background(), nil)
		assert.Error(t, err)
	})
}

func TestList_AddAt(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		assert.NoError(t, l.AddAt(context.Background(), 0, "test1"))
		assert.NoError(t, l.AddAt(context.Background(), 1, "test2"))
		result, err := l.Get(context.Background(), 1)
		assert.NoError(t, err)
		assert.Equal(t, "test2", result)
	})
}

func TestList_AddAt_Error(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skipf("not necessary for 32bit")
	}
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		assert.Error(t, l.AddAt(context.Background(), -1, "test-negative"))
		x := int64(math.MaxInt32 + 1)
		assert.Error(t, l.AddAt(context.Background(), int(x), "test-overflow"))
	})
}

func TestList_AddAtNilElement(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		assert.Error(t, l.AddAt(context.Background(), 0, nil))
	})
}

func TestList_AddAll(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		added, err := l.AddAll(context.Background(), "1", "2")
		assert.NoError(t, err)
		assert.True(t, added)
		res1, err := l.Get(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, "1", res1)
		res2, err := l.Get(context.Background(), 1)
		assert.NoError(t, err)
		assert.Equal(t, "2", res2)
	})
}

func TestList_AddAllWithNilElement(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.AddAll(context.Background(), "1", nil)
		assert.Error(t, err)
	})
}

func TestList_AddAllAt(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		assert.NoError(t, l.AddAt(context.Background(), 0, "0"))
		added, err := l.AddAllAt(context.Background(), 1, "1", "2")
		assert.NoError(t, err)
		assert.True(t, added)
		res1, err := l.Get(context.Background(), 1)
		assert.NoError(t, err)
		assert.Equal(t, "1", res1)
		res2, err := l.Get(context.Background(), 2)
		assert.NoError(t, err)
		assert.Equal(t, "2", res2)
	})
}

func TestList_AddAllAt_Error(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skipf("not necessary for 32bit")
	}
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.AddAllAt(context.Background(), -1, "negative")
		assert.Error(t, err)
		x := int64(math.MaxInt32 + 1)
		_, err = l.AddAllAt(context.Background(), int(x), "overflow")
		assert.Error(t, err)
	})
}

func TestList_AddAllAtWithNilElement(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.AddAllAt(context.Background(), 1, "0", nil)
		assert.Error(t, err)
	})
}

func TestList_Clear(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.AddAll(context.Background(), "1", "2")
		assert.NoError(t, err)
		assert.NoError(t, l.Clear(context.Background()))
		size, err := l.Size(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 0, size)
	})
}

func TestList_Contains(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.AddAll(context.Background(), "1", "2")
		assert.NoError(t, err)
		found, err := l.Contains(context.Background(), "1")
		assert.NoError(t, err)
		assert.True(t, found)
	})
}

func TestList_ContainsWithNilElement(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.Contains(context.Background(), nil)
		assert.Error(t, err)
	})
}

func TestList_ContainsAll(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		all := []interface{}{"1", "2"}
		_, err := l.AddAll(context.Background(), all...)
		assert.NoError(t, err)
		found, err := l.ContainsAll(context.Background(), all...)
		assert.NoError(t, err)
		assert.True(t, found)
	})
}

func TestList_ContainsAllWithNilElement(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.ContainsAll(context.Background(), nil)
		assert.Error(t, err)
	})
}

func TestList_ToSlice(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		res, err := l.GetAll(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 0, len(res))
		all := []interface{}{"1", "2"}
		_, err = l.AddAll(context.Background(), all...)
		assert.NoError(t, err)
		res, err = l.GetAll(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, all[0], res[0])
		assert.Equal(t, all[1], res[1])
	})
}

func TestList_IndexOf(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.AddAll(context.Background(), "1", "2")
		assert.NoError(t, err)
		index, err := l.IndexOf(context.Background(), "2")
		assert.NoError(t, err)
		assert.Equal(t, 1, index)
	})
}

func TestList_IndexOfWithNilElement(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.IndexOf(context.Background(), nil)
		assert.Error(t, err)
	})
}

func TestList_IsEmpty(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		empty, err := l.IsEmpty(context.Background())
		assert.NoError(t, err)
		assert.True(t, empty)
	})
}

func TestList_LastIndexOf(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.AddAll(context.Background(), "1", "2", "2")
		assert.NoError(t, err)
		index, err := l.LastIndexOf(context.Background(), "2")
		assert.NoError(t, err)
		assert.Equal(t, 2, index)
	})
}

func TestList_LastIndexOfWithNilElement(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.LastIndexOf(context.Background(), nil)
		assert.Error(t, err)
	})
}

func TestList_Remove(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.Add(context.Background(), "1")
		assert.NoError(t, err)
		removed, err := l.Remove(context.Background(), "1")
		assert.NoError(t, err)
		assert.True(t, removed)
		removed, err = l.Remove(context.Background(), "2")
		assert.NoError(t, err)
		assert.False(t, removed)
	})
}

func TestList_RemoveWithNilElement(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.Remove(context.Background(), nil)
		assert.Error(t, err)
	})
}

func TestList_RemoveAt(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.Add(context.Background(), "1")
		assert.NoError(t, err)
		previous, err := l.RemoveAt(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, "1", previous)
	})
}

func TestList_RemoveAt_Error(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skipf("not necessary for 32bit")
	}
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.RemoveAt(context.Background(), -1)
		assert.Error(t, err)
		x := int64(math.MaxInt32 + 1)
		_, err = l.RemoveAt(context.Background(), int(x))
		assert.Error(t, err)
	})
}

func TestList_RemoveAll(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.AddAll(context.Background(), "1", "2", "3")
		assert.NoError(t, err)
		removedAll, err := l.RemoveAll(context.Background(), "2", "3")
		assert.NoError(t, err)
		assert.True(t, removedAll)
		found, err := l.Contains(context.Background(), "1")
		assert.NoError(t, err)
		assert.True(t, found)
	})
}

func TestList_RemoveAllWithNilElement(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.RemoveAll(context.Background(), nil, "1", "2")
		assert.Error(t, err)
	})
}

func TestList_RetainAll(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.AddAll(context.Background(), "1", "2", "3")
		assert.NoError(t, err)
		changed, err := l.RetainAll(context.Background(), "2", "3")
		assert.NoError(t, err)
		assert.True(t, changed)
		found, err := l.Contains(context.Background(), "1")
		assert.NoError(t, err)
		assert.False(t, found)
	})
}

func TestList_RetainAllWithNilElement(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.RetainAll(context.Background(), nil, "1", "2")
		assert.Error(t, err)
	})
}

func TestList_Size(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.AddAll(context.Background(), "1", "2", "3")
		assert.NoError(t, err)
		size, err := l.Size(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 3, size)
	})
}

func TestList_Get(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.Add(context.Background(), "1")
		assert.NoError(t, err)
		res, err := l.Get(context.Background(), 0)
		assert.NoError(t, err)
		assert.Equal(t, "1", res)
	})
}

func TestList_Get_Error(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skipf("not necessary for 32bit")
	}
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.Get(context.Background(), -1)
		assert.Error(t, err)
		x := int64(math.MaxInt32 + 1)
		_, err = l.Get(context.Background(), int(x))
		assert.Error(t, err)
	})
}

func TestList_Set(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.AddAll(context.Background(), "1", "2", "3")
		assert.NoError(t, err)
		_, err = l.Set(context.Background(), 1, "13")
		assert.NoError(t, err)
		res, err := l.Get(context.Background(), 1)
		assert.NoError(t, err)
		assert.Equal(t, "13", res)
	})
}

func TestList_Set_Error(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skipf("not necessary for 32bit")
	}
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.Set(context.Background(), -1, "negative")
		assert.Error(t, err)
		x := int64(math.MaxInt32 + 1)
		_, err = l.Set(context.Background(), int(x), "overflow")
		assert.Error(t, err)
	})
}

func TestList_SetWithNilElement(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.Set(context.Background(), 0, nil)
		assert.Error(t, err)
	})
}

func TestList_SubList(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.AddAll(context.Background(), "1", "2", "3")
		assert.NoError(t, err)
		res, err := l.SubList(context.Background(), 1, 3)
		assert.NoError(t, err)
		assert.Equal(t, "2", res[0])
		assert.Equal(t, "3", res[1])
	})
}

func TestList_SubList_Error(t *testing.T) {
	if runtime.GOARCH == "386" {
		t.Skipf("not necessary for 32bit")
	}
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.SubList(context.Background(), -1, 3)
		assert.Error(t, err)
		_, err = l.SubList(context.Background(), 1, -3)
		assert.Error(t, err)
		x := int64(math.MaxInt32 + 1)
		_, err = l.SubList(context.Background(), int(x), 3)
		assert.Error(t, err)
		_, err = l.SubList(context.Background(), 1, int(x))
		assert.Error(t, err)

	})
}

func TestList_SetWithoutItem(t *testing.T) {
	it.ListTester(t, func(t *testing.T, l *hz.List) {
		_, err := l.Set(context.Background(), 1, "a")
		assert.Error(t, err)
	})
}
