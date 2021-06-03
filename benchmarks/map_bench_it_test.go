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

package benchmarks_test

import (
	"fmt"
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

const kb = 1024

func BenchmarkMap_SetDifferentKeyValue(b *testing.B) {
	it.MapBenchmarker(b, nil, func(b *testing.B, m *hz.Map) {
		for i := 0; i < b.N; i++ {
			key, value := makeKeyValue(i)
			it.Must(m.Set(nil, key, value))
		}
	})
}

func BenchmarkMap_SetGetDifferentKeyValue(b *testing.B) {
	it.MapBenchmarker(b, nil, func(b *testing.B, m *hz.Map) {
		for i := 0; i < b.N; i++ {
			key, value := makeKeyValue(i)
			it.Must(m.Set(nil, key, value))
			it.MustValue(m.Get(nil, key))
		}
	})
}

func BenchmarkMap_SetSameKeyValue(b *testing.B) {
	it.MapBenchmarker(b, nil, func(b *testing.B, m *hz.Map) {
		for i := 0; i < b.N; i++ {
			it.Must(m.Set(nil, "key", "value"))
		}
	})
}

func BenchmarkMap_SetLargePayload_128KB(b *testing.B) {
	payload := makeByteArrayPayload(128 * kb)
	it.MapBenchmarker(b, nil, func(b *testing.B, m *hz.Map) {
		for i := 0; i < b.N; i++ {
			it.Must(m.Set(nil, "key", payload))
		}
	})
}

func BenchmarkMap_GetSameKeyValue(b *testing.B) {
	fixture := func(m *hz.Map) {
		it.Must(m.Set(nil, "key", "value"))
	}
	it.MapBenchmarker(b, fixture, func(b *testing.B, m *hz.Map) {
		for i := 0; i < b.N; i++ {
			it.MustValue(m.Get(nil, "key"))
		}
	})
}

func BenchmarkMap_GetLargePayload_128KB(b *testing.B) {
	fixture := func(m *hz.Map) {
		payload := makeByteArrayPayload(128 * kb)
		it.Must(m.Set(nil, "key", payload))
	}
	it.MapBenchmarker(b, fixture, func(b *testing.B, m *hz.Map) {
		for i := 0; i < b.N; i++ {
			it.MustValue(m.Get(nil, "key"))
		}
	})
}

func BenchmarkMap_SetParallel(b *testing.B) {
	it.MapBenchmarker(b, nil, func(b *testing.B, m *hz.Map) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				it.Must(m.Set(nil, "key", "value"))
			}
		})
	})
}

func BenchmarkMap_SetParallelLargePayload_128KB(b *testing.B) {
	it.MapBenchmarker(b, nil, func(b *testing.B, m *hz.Map) {
		payload := makeByteArrayPayload(128 * kb)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				it.Must(m.Set(nil, "key", payload))
			}
		})
	})
}

func BenchmarkMap_GetParallel(b *testing.B) {
	fixture := func(m *hz.Map) {
		it.Must(m.Set(nil, "key", "value"))
	}
	it.MapBenchmarker(b, fixture, func(b *testing.B, m *hz.Map) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				it.MustValue(m.Get(nil, "key"))
			}
		})
	})
}

func makeByteArrayPayload(size int) []byte {
	payload := make([]byte, size)
	for i := 0; i < len(payload); i++ {
		payload[i] = byte(i)
	}
	return payload
}

func makeKeyValue(i int) (string, string) {
	key := fmt.Sprintf("key-%d", i)
	value := fmt.Sprintf("value-%d", i)
	return key, value
}
