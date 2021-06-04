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

package predicate_test

import (
	"context"
	"fmt"
	"testing"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/stretchr/testify/assert"
)

func TestPredicate_And(t *testing.T) {
	pred := predicate.And(
		predicate.Greater("a", 5),
		predicate.Equal("b", "value1"),
	)
	target := []interface{}{
		serialization.JSON(`{"a": 10, "b": "value1", "c": true}`),
	}
	check(t, pred, target)
}

func TestPredicate_Between(t *testing.T) {
	pred := predicate.Between("a", 6, 14)
	target := []interface{}{
		serialization.JSON(`{"a": 10, "b": "value1", "c": true}`),
	}
	check(t, pred, target)
}

func TestPredicate_Equal(t *testing.T) {
	pred := predicate.Equal("b", "value1")
	target := []interface{}{
		serialization.JSON(`{"a": 5, "b": "value1", "c": false}`),
		serialization.JSON(`{"a": 10, "b": "value1", "c": true}`),
	}
	check(t, pred, target)
}

func TestPredicate_False(t *testing.T) {
	pred := predicate.False()
	target := []interface{}{}
	check(t, pred, target)
}

func TestPredicate_Greater(t *testing.T) {
	pred := predicate.Greater("a", 10)
	target := []interface{}{
		serialization.JSON(`{"a": 15, "b": "value2", "c": false}`),
	}
	check(t, pred, target)
}

func TestPredicate_GreaterOrEqual(t *testing.T) {
	pred := predicate.GreaterOrEqual("a", 10)
	target := []interface{}{
		serialization.JSON(`{"a": 10, "b": "value1", "c": true}`),
		serialization.JSON(`{"a": 15, "b": "value2", "c": false}`),
	}
	check(t, pred, target)
}

func TestPredicate_ILike(t *testing.T) {
	pred := predicate.ILike("b", "V%1")
	target := []interface{}{
		serialization.JSON(`{"a": 5, "b": "value1", "c": false}`),
		serialization.JSON(`{"a": 10, "b": "value1", "c": true}`),
	}
	check(t, pred, target)
}

func TestPredicate_In(t *testing.T) {
	pred := predicate.In("a", 5, 10)
	target := []interface{}{
		serialization.JSON(`{"a": 5, "b": "value1", "c": false}`),
		serialization.JSON(`{"a": 10, "b": "value1", "c": true}`),
	}
	check(t, pred, target)
}

func TestPredicate_InstanceOf(t *testing.T) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		it.Must(m.Set(context.Background(), "k1", "foo"))
		it.Must(m.Set(context.Background(), "k2", true))
		it.Must(m.Set(context.Background(), "k3", 66))
		pred := predicate.InstanceOf("java.lang.Boolean")
		values := it.MustValue(m.GetValuesWithPredicate(context.Background(), pred))
		target := []interface{}{true}
		if !assert.Equal(t, target, values) {
			t.FailNow()
		}
	})
}

func TestPredicate_Like(t *testing.T) {
	pred := predicate.Like("b", "v%1")
	target := []interface{}{
		serialization.JSON(`{"a": 5, "b": "value1", "c": false}`),
		serialization.JSON(`{"a": 10, "b": "value1", "c": true}`),
	}
	check(t, pred, target)
}

func TestPredicate_Less(t *testing.T) {
	pred := predicate.Less("a", 10)
	target := []interface{}{
		serialization.JSON(`{"a": 5, "b": "value1", "c": false}`),
	}
	check(t, pred, target)
}

func TestPredicate_LessOrEqual(t *testing.T) {
	pred := predicate.LessOrEqual("a", 10)
	target := []interface{}{
		serialization.JSON(`{"a": 5, "b": "value1", "c": false}`),
		serialization.JSON(`{"a": 10, "b": "value1", "c": true}`),
	}
	check(t, pred, target)
}

func TestPredicate_Not(t *testing.T) {
	pred := predicate.Not(predicate.Equal("b", "value1"))
	target := []interface{}{
		serialization.JSON(`{"a": 15, "b": "value2", "c": false}`),
	}
	check(t, pred, target)
}

func TestPredicate_NotEqual(t *testing.T) {
	pred := predicate.NotEqual("b", "value1")
	target := []interface{}{
		serialization.JSON(`{"a": 15, "b": "value2", "c": false}`),
	}
	check(t, pred, target)
}

func TestPredicate_Or(t *testing.T) {
	pred := predicate.Or(
		predicate.Greater("a", 10),
		predicate.Less("a", 10),
	)
	target := []interface{}{
		serialization.JSON(`{"a": 5, "b": "value1", "c": false}`),
		serialization.JSON(`{"a": 15, "b": "value2", "c": false}`),
	}
	check(t, pred, target)
}

func TestPredicate_Regex(t *testing.T) {
	pred := predicate.Regex("b", "[a-z]+e1$")
	target := []interface{}{
		serialization.JSON(`{"a": 5, "b": "value1", "c": false}`),
		serialization.JSON(`{"a": 10, "b": "value1", "c": true}`),
	}
	check(t, pred, target)
}

func TestPredicate_SQL(t *testing.T) {
	pred := predicate.SQL("b != 'value1'")
	target := []interface{}{
		serialization.JSON(`{"a": 15, "b": "value2", "c": false}`),
	}
	check(t, pred, target)
}

func TestPredicate_True(t *testing.T) {
	pred := predicate.True()
	target := []interface{}{
		serialization.JSON(`{"a": 5, "b": "value1", "c": false}`),
		serialization.JSON(`{"a": 10, "b": "value1", "c": true}`),
		serialization.JSON(`{"a": 15, "b": "value2", "c": false}`),
	}
	check(t, pred, target)

}

func check(t *testing.T, pred predicate.Predicate, target []interface{}) {
	it.MapTester(t, func(t *testing.T, m *hz.Map) {
		createFixture(m)
		values := it.MustValue(m.GetValuesWithPredicate(context.Background(), pred))
		if !assert.Subset(t, target, values) {
			t.FailNow()
		}
		if !assert.Subset(t, values, target) {
			t.FailNow()
		}
	})
}

func createFixture(m *hz.Map) {
	values := []interface{}{
		serialization.JSON(`{"a": 5, "b": "value1", "c": false}`),
		serialization.JSON(`{"a": 10, "b": "value1", "c": true}`),
		serialization.JSON(`{"a": 15, "b": "value2", "c": false}`),
	}
	for i, v := range values {
		it.Must(m.Set(context.Background(), fmt.Sprintf("k%d", i), v))
	}
	if it.MustValue(m.Size(context.Background())) != len(values) {
		panic(fmt.Sprintf("expected %d values", len(values)))
	}
}
