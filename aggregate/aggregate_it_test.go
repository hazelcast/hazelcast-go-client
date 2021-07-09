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

package aggregate_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/aggregate"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestCount(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(ctx, "k2", &it.SamplePortable{A: "bar", B: 30}))
		it.MustValue(m.Put(ctx, "k3", &it.SamplePortable{A: "zoo", B: 30}))
		result, err := m.Aggregate(ctx, aggregate.Count("B"))
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(3), result)
	})
}

func TestCountAll(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(ctx, "k2", &it.SamplePortable{A: "bar", B: 30}))
		it.MustValue(m.Put(ctx, "k3", &it.SamplePortable{A: "zoo", B: 30}))
		it.MustValue(m.Put(ctx, "k4", 123))
		result, err := m.Aggregate(ctx, aggregate.CountAll())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(4), result)
	})
}

func TestDistinctValues(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(ctx, "k2", &it.SamplePortable{A: "bar", B: 30}))
		it.MustValue(m.Put(ctx, "k3", &it.SamplePortable{A: "zoo", B: 30}))
		result, err := m.Aggregate(ctx, aggregate.DistinctValues("B"))
		if err != nil {
			t.Fatal(err)
		}
		target := []interface{}{int32(10), int32(30)}
		assert.ElementsMatch(t, target, result)
	})
}

func TestDistinctValuesAll(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(ctx, "k2", &it.SamplePortable{A: "bar", B: 30}))
		it.MustValue(m.Put(ctx, "k3", &it.SamplePortable{A: "zoo", B: 30}))
		result, err := m.Aggregate(ctx, aggregate.DistinctValuesAll())
		if err != nil {
			t.Fatal(err)
		}
		target := []interface{}{&it.SamplePortable{A: "foo", B: 10}, &it.SamplePortable{A: "bar", B: 30}, &it.SamplePortable{A: "zoo", B: 30}}
		assert.ElementsMatch(t, target, result)
	})
}

func TestDoubleAverage(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(ctx, "k2", &it.SamplePortable{A: "bar", B: 25}))
		result, err := m.Aggregate(ctx, aggregate.DoubleAverage("B"))
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, float64(17.5), result)
	})
}

func TestDoubleAverageAll(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", 10))
		it.MustValue(m.Put(ctx, "k2", 25))
		result, err := m.Aggregate(ctx, aggregate.DoubleAverageAll())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, float64(17.5), result)
	})
}

func TestDoubleAverageAll_Mixed(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", 10.4))
		it.MustValue(m.Put(ctx, "k2", int64(25)))
		result, err := m.Aggregate(ctx, aggregate.DoubleAverageAll())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, float64(17.7), result)
	})
}

func TestDoubleSum(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(ctx, "k2", &it.SamplePortable{A: "bar", B: 25}))
		result, err := m.Aggregate(ctx, aggregate.DoubleSum("B"))
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, float64(35), result)
	})
}

func TestDoubleSumAll(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", 10))
		it.MustValue(m.Put(ctx, "k2", 25))
		result, err := m.Aggregate(ctx, aggregate.DoubleSumAll())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, float64(35), result)
	})
}

func TestDoubleSumAll_Mixed(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", 10.4))
		it.MustValue(m.Put(ctx, "k2", int64(25)))
		result, err := m.Aggregate(ctx, aggregate.DoubleSumAll())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, float64(35.4), result)
	})
}

func TestLongAverage(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(ctx, "k2", &it.SamplePortable{A: "bar", B: 25}))
		result, err := m.Aggregate(ctx, aggregate.LongAverage("B"))
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, float64(17.5), result)
	})
}

func TestLongAverageAll(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", 10))
		it.MustValue(m.Put(ctx, "k2", 25))
		result, err := m.Aggregate(ctx, aggregate.LongAverageAll())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, float64(17.5), result)
	})
}

func TestLongSum(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(ctx, "k2", &it.SamplePortable{A: "bar", B: 25}))
		result, err := m.Aggregate(ctx, aggregate.LongSum("B"))
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(35), result)
	})
}

func TestLongSumAll(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", 10))
		it.MustValue(m.Put(ctx, "k2", 25))
		result, err := m.Aggregate(ctx, aggregate.LongSumAll())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(35), result)
	})
}

func TestIntAverage(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(ctx, "k2", &it.SamplePortable{A: "bar", B: 25}))
		result, err := m.Aggregate(ctx, aggregate.IntAverage("B"))
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, float64(17.5), result)
	})
}

func TestIntAverageAll(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", 10))
		it.MustValue(m.Put(ctx, "k2", 25))
		result, err := m.Aggregate(ctx, aggregate.IntAverageAll())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, float64(17.5), result)
	})
}

func TestIntSum(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(ctx, "k2", &it.SamplePortable{A: "bar", B: 25}))
		result, err := m.Aggregate(ctx, aggregate.IntSum("B"))
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(35), result)
	})
}

func TestIntSumAll(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", 10))
		it.MustValue(m.Put(ctx, "k2", 25))
		result, err := m.Aggregate(ctx, aggregate.IntSumAll())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(35), result)
	})
}

func TestMin(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(ctx, "k2", &it.SamplePortable{A: "bar", B: 30}))
		it.MustValue(m.Put(ctx, "k3", &it.SamplePortable{A: "zoo", B: 30}))
		result, err := m.Aggregate(ctx, aggregate.Min("B"))
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int32(10), result)
	})
}

func TestMinAll(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", 10))
		it.MustValue(m.Put(ctx, "k2", 30))
		it.MustValue(m.Put(ctx, "k3", 30))
		result, err := m.Aggregate(ctx, aggregate.MinAll())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(10), result)
	})
}

func TestMax(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put(ctx, "k2", &it.SamplePortable{A: "bar", B: 30}))
		it.MustValue(m.Put(ctx, "k3", &it.SamplePortable{A: "zoo", B: 30}))
		result, err := m.Aggregate(ctx, aggregate.Max("B"))
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int32(30), result)
	})
}

func TestMaxAll(t *testing.T) {
	cbCallback := func(config *hz.Config) {
		config.Serialization.SetPortableFactories(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfig(t, cbCallback, func(t *testing.T, m *hz.Map) {
		ctx := context.Background()
		it.MustValue(m.Put(ctx, "k1", 10))
		it.MustValue(m.Put(ctx, "k2", 30))
		it.MustValue(m.Put(ctx, "k3", 30))
		result, err := m.Aggregate(ctx, aggregate.MaxAll())
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, int64(30), result)
	})
}
