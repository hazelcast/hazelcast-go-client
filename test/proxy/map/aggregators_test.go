// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package map1

import (
	"testing"

	"reflect"

	"github.com/hazelcast/hazelcast-go-client/core/aggregator"
	"github.com/hazelcast/hazelcast-go-client/core/predicate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testSerializationOfAggregation(t *testing.T, aggregator interface{}) {
	aggregationData, err := serializationService.ToData(aggregator)
	if err != nil {
		t.Fatal(err)
	}
	retAggregator, err := serializationService.ToObject(aggregationData)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(aggregator, retAggregator.(interface{})) {
		t.Fatalf("%s is not serialized/deseserialized correctly", reflect.TypeOf(aggregator))
	}
}

func TestMapProxy_AggregateNilAggregator(t *testing.T) {
	defer mp.Clear()
	_, err := mp.Aggregate(nil)
	require.Errorf(t, err, "Map.Aggreate should return an error for nil aggregator")
}

func TestMapProxy_AggregateWithPredicateNilAggregator(t *testing.T) {
	defer mp.Clear()
	_, err := mp.AggregateWithPredicate(nil,
		predicate.GreaterEqual("this", 4))
	require.Errorf(t, err, "Map.AggreateWithPredicate should return an error for nil aggregator")
}

func TestMapProxy_AggregateWithPredicateNilPredicate(t *testing.T) {
	defer mp.Clear()
	countAgg, _ := aggregator.Count("this")
	_, err := mp.AggregateWithPredicate(countAgg, nil)
	require.Errorf(t, err, "Map.AggreateWithPredicate should return an error for nil predicate")
}

func TestMapProxy_Aggregate_Count(t *testing.T) {
	defer mp.Clear()
	expectedCount := 50
	FillMapWithInt64(expectedCount)
	countAgg, _ := aggregator.Count("this")
	result, err := mp.Aggregate(countAgg)
	require.NoError(t, err)
	assert.Equalf(t, result, int64(expectedCount), "Map.Aggregate Count failed")
	testSerializationOfAggregation(t, countAgg)
}

func TestMapProxy_Aggregate_CountEmpty(t *testing.T) {
	_, err := aggregator.Count("")
	require.Errorf(t, err, "aggregator.Count should return an error for empty attributePath")
}

func TestMapProxy_Aggregate_CountWithPredicate(t *testing.T) {
	defer mp.Clear()
	FillMapWithInt64(50)
	countAgg, _ := aggregator.Count("this")
	result, err := mp.AggregateWithPredicate(countAgg,
		predicate.GreaterEqual("this", 1))
	require.NoError(t, err)
	assert.Equalf(t, result, int64(49), "Map.AggregateWithPredicate Count failed")
}

func TestMapProxy_Aggregate_FixedPointSum(t *testing.T) {
	defer mp.Clear()
	FillMapWithInt64(50)
	fixedPointSumAgg, _ := aggregator.FixedPointSum("this")
	result, err := mp.Aggregate(fixedPointSumAgg)
	require.NoError(t, err)
	assert.Equalf(t, result, int64(1225), "Map.Aggregate FixedPointSum failed")
	testSerializationOfAggregation(t, fixedPointSumAgg)
}

func TestMapProxy_Aggregate_FixedPointSumEmpty(t *testing.T) {
	_, err := aggregator.FixedPointSum("")
	require.Errorf(t, err, "aggregator.FixedPointSum should return an error for empty attributePath")
}

func TestMapProxy_Aggregate_FloatingPointSum(t *testing.T) {
	defer mp.Clear()
	FillMapWithFloat64(50)
	floatingPointSumAgg, _ := aggregator.FloatingPointSum("this")
	result, err := mp.Aggregate(floatingPointSumAgg)
	require.NoError(t, err)
	assert.Equalf(t, result, float64(1225), "Map.Aggregate FloatingPointSum failed")
	testSerializationOfAggregation(t, floatingPointSumAgg)
}

func TestMapProxy_Aggregate_FloatingPointSumEmpty(t *testing.T) {
	_, err := aggregator.FloatingPointSum("")
	require.Errorf(t, err, "aggregator.FloatingPointSum should return an error for empty attributePath")
}

func TestMapProxy_Aggregate_FloatingPointSumWithPredicate(t *testing.T) {
	defer mp.Clear()
	FillMapWithFloat64(50)
	FloatingPointSumAgg, _ := aggregator.FloatingPointSum("this")
	result, err := mp.AggregateWithPredicate(FloatingPointSumAgg,
		predicate.GreaterEqual("this", 47))
	require.NoError(t, err)
	assert.Equalf(t, result, float64(144), "Map.AggregateWithPredicate FloatingPointSum failed")
}

func TestMapProxy_Aggregate_Max(t *testing.T) {
	defer mp.Clear()
	FillMapWithInt64(50)
	maxAgg, _ := aggregator.Max("this")
	result, err := mp.Aggregate(maxAgg)
	require.NoError(t, err)
	assert.Equalf(t, result, int64(49), "Map.Aggregate Max failed")
	testSerializationOfAggregation(t, maxAgg)
}

func TestMapProxy_Aggregate_MaxEmpty(t *testing.T) {
	_, err := aggregator.Max("")
	require.Errorf(t, err, "aggregator.Max should return an error for empty attributePath")
}

func TestMapProxy_Aggregate_MaxWithPredicate(t *testing.T) {
	defer mp.Clear()
	FillMapWithInt64(50)
	maxAgg, _ := aggregator.Max("this")
	result, err := mp.AggregateWithPredicate(maxAgg,
		predicate.LessEqual("this", 3))
	require.NoError(t, err)
	assert.Equalf(t, result, int64(3), "Map.AggregateWithPredicate Max failed")
}

func TestMapProxy_Aggregate_Min(t *testing.T) {
	defer mp.Clear()
	FillMapWithInt64(50)
	minAgg, _ := aggregator.Min("this")
	result, err := mp.Aggregate(minAgg)
	require.NoError(t, err)
	assert.Equalf(t, result, int64(0), "Map.Aggregate Min failed")
	testSerializationOfAggregation(t, minAgg)
}

func TestMapProxy_Aggregate_MinEmpty(t *testing.T) {
	_, err := aggregator.Min("")
	require.Errorf(t, err, "aggregator.Min should return an error for empty attributePath")
}

func TestMapProxy_Aggregate_MinWithPredicate(t *testing.T) {
	defer mp.Clear()
	FillMapWithInt64(50)
	minAgg, _ := aggregator.Min("this")
	result, err := mp.AggregateWithPredicate(minAgg,
		predicate.GreaterEqual("this", 3))
	require.NoError(t, err)
	assert.Equalf(t, result, int64(3), "Map.AggregateWithPredicate Min failed")
}

func TestMapProxy_Aggregate_Int64Average(t *testing.T) {
	defer mp.Clear()
	FillMapWithInt64(50)
	int64AvgAgg, _ := aggregator.Int64Average("this")
	result, err := mp.Aggregate(int64AvgAgg)
	require.NoError(t, err)
	assert.Equalf(t, result, float64(24.5), "Map.Aggregate Int64Average failed")
	testSerializationOfAggregation(t, int64AvgAgg)
}

func TestMapProxy_Aggregate_Int64AverageEmpty(t *testing.T) {
	_, err := aggregator.Count("")
	require.Errorf(t, err, "aggregator.Int64Average should return an error for empty attributePath")
}

func TestMapProxy_Aggregate_Int64Sum(t *testing.T) {
	defer mp.Clear()
	FillMapWithInt64(50)
	int64SumAgg, _ := aggregator.Int64Sum("this")
	result, err := mp.Aggregate(int64SumAgg)
	require.NoError(t, err)
	assert.Equalf(t, result, int64(1225), "Map.Aggregate Int64Sum failed")
	testSerializationOfAggregation(t, int64SumAgg)
}

func TestMapProxy_Aggregate_Int64SumEmpty(t *testing.T) {
	_, err := aggregator.Int64Sum("")
	require.Errorf(t, err, "aggregator.Int64Sum should return an error for empty attributePath")
}

func TestMapProxy_Aggregate_Float64Sum(t *testing.T) {
	defer mp.Clear()
	FillMapWithFloat64(50)
	float64SumAgg, _ := aggregator.Float64Sum("this")
	result, err := mp.Aggregate(float64SumAgg)
	require.NoError(t, err)
	assert.Equalf(t, result, float64(1225), "Map.Aggregate Float64Sum failed")
	testSerializationOfAggregation(t, float64SumAgg)
}

func TestMapProxy_Aggregate_Float64SumEmpty(t *testing.T) {
	_, err := aggregator.Float64Sum("")
	require.Errorf(t, err, "aggregator.Float64Sum should return an error for empty attributePath")
}

func TestMapProxy_Aggregate_Int32Average(t *testing.T) {
	defer mp.Clear()
	FillMapWithInt32(50)
	int32AvgAgg, _ := aggregator.Int32Average("this")
	result, err := mp.Aggregate(int32AvgAgg)
	require.NoError(t, err)
	assert.Equalf(t, result, float64(24.5), "Map.Aggregate Int32Average failed")
	testSerializationOfAggregation(t, int32AvgAgg)
}

func TestMapProxy_Aggregate_Int32AverageEmpty(t *testing.T) {
	_, err := aggregator.Int32Average("")
	require.Errorf(t, err, "aggregator.Int32Average should return an error for empty attributePath")
}

func TestMapProxy_Aggregate_Int32Sum(t *testing.T) {
	defer mp.Clear()
	FillMapWithInt32(50)
	int32SumAgg, _ := aggregator.Int32Sum("this")
	result, err := mp.Aggregate(int32SumAgg)
	require.NoError(t, err)
	assert.Equalf(t, result, int64(1225), "Map.Aggregate Int32Sum failed")
	testSerializationOfAggregation(t, int32SumAgg)
}

func TestMapProxy_Aggregate_Int32SumEmpty(t *testing.T) {
	_, err := aggregator.Int32Sum("")
	require.Errorf(t, err, "aggregator.Int32Sum should return an error for empty attributePath")
}

func TestMapProxy_Aggregate_Float64Average(t *testing.T) {
	defer mp.Clear()
	FillMapWithFloat64(50)
	float64AvgAgg, _ := aggregator.Float64Average("this")
	result, err := mp.Aggregate(float64AvgAgg)
	require.NoError(t, err)
	assert.Equalf(t, result, float64(24.5), "Map.Aggregate NumberAverage failed")
	testSerializationOfAggregation(t, float64AvgAgg)
}

func TestMapProxy_Aggregate_Float64AverageEmpty(t *testing.T) {
	_, err := aggregator.Float64Average("")
	require.Errorf(t, err, "aggregator.Float64Average should return an error for empty attributePath")
}

func FillMapWithFloat64(expectedCount int) {
	for i := 0; i < expectedCount; i++ {
		mp.Put(float64(i), float64(i))
	}
}

func FillMapWithInt64(expectedCount int) {
	for i := 0; i < expectedCount; i++ {
		mp.Put(i, i)
	}
}

func FillMapWithInt32(expectedCount int) {
	for i := 0; i < expectedCount; i++ {
		mp.Put(int32(i), int32(i))
	}
}
