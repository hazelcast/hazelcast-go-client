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

package serialization_test

import (
	"errors"
	"math"
	"math/big"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestCompactSerializer(t *testing.T) {
	testCases := []struct {
		name string
		f    func(t *testing.T)
	}{
		{name: "AddDuplicateField", f: addDuplicateFieldTest},
		{name: "NullableSerializer", f: nullableSerializerTest},
		{name: "PrimitiveArraySerializer", f: primitiveArraySerializerTest},
		{name: "OtherArraySerializer", f: otherArraySerializerTest},
		{name: "SliceWithDifferentTypes", f: sliceWithDifferentTypesTest},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, tc.f)
	}
}

func addDuplicateFieldTest(t *testing.T) {
	// ported from: com.hazelcast.internal.serialization.impl.compact.CompactSerializationTest#testSerializer_withDuplicateFieldNames
	var cfg pubserialization.Config
	cfg.Compact.SetSerializers(&duplicateWritingSerializer{})
	_, err := serialization.NewService(&cfg, nil)
	require.Error(t, err)
}

func sliceWithDifferentTypesTest(t *testing.T) {
	// ported from: testWritingArrayOfCompactGenericField_withDifferentItemTypes
	var cfg pubserialization.Config
	cfg.Compact.SetSerializers(&sliceWithDifferentTypesSerializer{}, &BitsDTOSerializer{}, &InnerDTOSerializer{})
	ss, err := serialization.NewService(&cfg, nil)
	require.NoError(t, err)
	_, err = ss.ToData(sliceWithDifferentTypes([]interface{}{BitsDTO{}, InnerDTO{}}))
	require.True(t, errors.Is(err, hzerrors.ErrHazelcastSerialization))
}

type duplicatedType struct{}

type duplicateWritingSerializer struct{}

func (d duplicateWritingSerializer) Type() reflect.Type {
	return reflect.TypeOf(duplicatedType{})
}

func (d duplicateWritingSerializer) TypeName() string {
	return "duplicated"
}

func (d duplicateWritingSerializer) Read(reader pubserialization.CompactReader) interface{} {
	return duplicatedType{}
}

func (d duplicateWritingSerializer) Write(writer pubserialization.CompactWriter, value interface{}) {
	writer.WriteInt32("bar", 1)
	writer.WriteInt32("bar", 1)
}

type sliceWithDifferentTypes []interface{}

type sliceWithDifferentTypesSerializer struct{}

func (s sliceWithDifferentTypesSerializer) Type() reflect.Type {
	return reflect.TypeOf(sliceWithDifferentTypes{})
}

func (s sliceWithDifferentTypesSerializer) TypeName() string {
	return "sliceWithDifferentTypes"
}

func (s sliceWithDifferentTypesSerializer) Read(reader pubserialization.CompactReader) interface{} {
	panic("not implemented")
}

func (s sliceWithDifferentTypesSerializer) Write(writer pubserialization.CompactWriter, value interface{}) {
	v := []interface{}(value.(sliceWithDifferentTypes))
	writer.WriteArrayOfCompact("f", v)
}

type primitiveArrays struct {
	emptyBoolArray         []bool
	fullBoolArray          []bool
	emptyInt8Array         []int8
	fullInt8Array          []int8
	emptyInt16Array        []int16
	fullInt16Array         []int16
	emptyInt32Array        []int32
	fullInt32Array         []int32
	emptyInt64Array        []int64
	fullInt64Array         []int64
	emptyFloat32Array      []float32
	fullFloat32Array       []float32
	emptyFloat64Array      []float64
	fullFloat64Array       []float64
	emptyNullableBoolArray []*bool
	fullNullableBoolArray  []*bool
	emptyNullableInt8Array []*int8
	fullNullableInt8Array  []*int8
}

type primitiveArraysSerializer struct{}

func (s primitiveArraysSerializer) Type() reflect.Type {
	return reflect.TypeOf(primitiveArrays{})
}

func (s primitiveArraysSerializer) TypeName() string {
	return "primitiveArrays"
}

func (s primitiveArraysSerializer) Read(r pubserialization.CompactReader) interface{} {
	return primitiveArrays{
		emptyBoolArray:         r.ReadArrayOfBoolean("emptyBoolArray"),
		fullBoolArray:          r.ReadArrayOfBoolean("fullBoolArray"),
		emptyInt8Array:         r.ReadArrayOfInt8("emptyInt8Array"),
		fullInt8Array:          r.ReadArrayOfInt8("fullInt8Array"),
		emptyInt16Array:        r.ReadArrayOfInt16("emptyInt16Array"),
		fullInt16Array:         r.ReadArrayOfInt16("fullInt16Array"),
		emptyInt32Array:        r.ReadArrayOfInt32("emptyInt32Array"),
		fullInt32Array:         r.ReadArrayOfInt32("fullInt32Array"),
		emptyInt64Array:        r.ReadArrayOfInt64("emptyInt64Array"),
		fullInt64Array:         r.ReadArrayOfInt64("fullInt64Array"),
		emptyFloat32Array:      r.ReadArrayOfFloat32("emptyFloat32Array"),
		fullFloat32Array:       r.ReadArrayOfFloat32("fullFloat32Array"),
		emptyFloat64Array:      r.ReadArrayOfFloat64("emptyFloat64Array"),
		fullFloat64Array:       r.ReadArrayOfFloat64("fullFloat64Array"),
		emptyNullableBoolArray: r.ReadArrayOfNullableBoolean("emptyNullableBoolArray"),
		fullNullableBoolArray:  r.ReadArrayOfNullableBoolean("fullNullableBoolArray"),
		emptyNullableInt8Array: r.ReadArrayOfNullableInt8("emptyNullableInt8Array"),
		fullNullableInt8Array:  r.ReadArrayOfNullableInt8("fullNullableInt8Array"),
	}
}

func (s primitiveArraysSerializer) Write(w pubserialization.CompactWriter, v interface{}) {
	vv := v.(primitiveArrays)
	w.WriteArrayOfBoolean("emptyBoolArray", vv.emptyBoolArray)
	w.WriteArrayOfBoolean("fullBoolArray", vv.fullBoolArray)
	w.WriteArrayOfInt8("emptyInt8Array", vv.emptyInt8Array)
	w.WriteArrayOfInt8("fullInt8Array", vv.fullInt8Array)
	w.WriteArrayOfInt16("emptyInt16Array", vv.emptyInt16Array)
	w.WriteArrayOfInt16("fullInt16Array", vv.fullInt16Array)
	w.WriteArrayOfInt32("emptyInt32Array", vv.emptyInt32Array)
	w.WriteArrayOfInt32("fullInt32Array", vv.fullInt32Array)
	w.WriteArrayOfInt64("emptyInt64Array", vv.emptyInt64Array)
	w.WriteArrayOfInt64("fullInt64Array", vv.fullInt64Array)
	w.WriteArrayOfFloat32("emptyFloat32Array", vv.emptyFloat32Array)
	w.WriteArrayOfFloat32("fullFloat32Array", vv.fullFloat32Array)
	w.WriteArrayOfFloat64("emptyFloat64Array", vv.emptyFloat64Array)
	w.WriteArrayOfFloat64("fullFloat64Array", vv.fullFloat64Array)
	w.WriteArrayOfNullableBoolean("fullNullableBoolArray", vv.fullNullableBoolArray)
	w.WriteArrayOfNullableBoolean("emptyNullableBoolArray", vv.emptyNullableBoolArray)
	w.WriteArrayOfNullableInt8("emptyNullableInt8Array", vv.emptyNullableInt8Array)
	w.WriteArrayOfNullableInt8("fullNullableInt8Array", vv.fullNullableInt8Array)
}

func primitiveArraySerializerTest(t *testing.T) {
	var cfg pubserialization.Config
	cfg.Compact.SetSerializers(&primitiveArraysSerializer{})
	ss := mustSerializationService(serialization.NewService(&cfg, nil))
	b := true
	i8 := int8(8)
	target := primitiveArrays{
		fullBoolArray:         []bool{true, false},
		fullInt8Array:         []int8{math.MinInt8, 0, math.MaxInt8},
		fullInt16Array:        []int16{math.MinInt16, 0, math.MaxInt16},
		fullInt32Array:        []int32{math.MinInt32, 0, math.MaxInt32},
		fullInt64Array:        []int64{math.MinInt64, 0, math.MinInt64},
		fullNullableBoolArray: []*bool{&b},
		fullNullableInt8Array: []*int8{&i8},
	}
	data := mustData(ss.ToData(target))
	v := it.MustValue(ss.ToObject(data))
	require.Equal(t, target, v)
}

type otherArrays struct {
	emptyDecimalArray []*types.Decimal
	fullDecimalArray  []*types.Decimal
}

type otherArraysSerializer struct{}

func (s otherArraysSerializer) Type() reflect.Type {
	return reflect.TypeOf(otherArrays{})
}

func (s otherArraysSerializer) TypeName() string {
	return "otherArrays"
}

func (s otherArraysSerializer) Read(r pubserialization.CompactReader) interface{} {
	return otherArrays{
		emptyDecimalArray: r.ReadArrayOfDecimal("emptyDecimalArray"),
		fullDecimalArray:  r.ReadArrayOfDecimal("fullDecimalArray"),
	}
}

func (s otherArraysSerializer) Write(w pubserialization.CompactWriter, v interface{}) {
	vv := v.(otherArrays)
	w.WriteArrayOfDecimal("emptyDecimalArray", vv.emptyDecimalArray)
	w.WriteArrayOfDecimal("fullDecimalArray", vv.fullDecimalArray)
}

func otherArraySerializerTest(t *testing.T) {
	var cfg pubserialization.Config
	cfg.Compact.SetSerializers(&otherArraysSerializer{})
	ss := mustSerializationService(serialization.NewService(&cfg, nil))
	decimal := types.NewDecimal(big.NewInt(100), 10)
	target := otherArrays{
		fullDecimalArray: []*types.Decimal{&decimal},
	}
	data := mustData(ss.ToData(target))
	v := it.MustValue(ss.ToObject(data))
	require.Equal(t, target, v)
}

type nullables struct {
	nilBool    *bool
	nilInt8    *int8
	nilInt16   *int16
	nilInt32   *int32
	nilInt64   *int64
	nilFloat32 *float32
	nilFloat64 *float64
}

type nullablesSerializer struct{}

func (s nullablesSerializer) Type() reflect.Type {
	return reflect.TypeOf(nullables{})
}

func (s nullablesSerializer) TypeName() string {
	return "nullables"
}

func (s nullablesSerializer) Read(r pubserialization.CompactReader) interface{} {
	return nullables{
		nilBool:    r.ReadNullableBoolean("nilBool"),
		nilInt8:    r.ReadNullableInt8("nilInt8"),
		nilInt16:   r.ReadNullableInt16("nilInt16"),
		nilInt32:   r.ReadNullableInt32("nilInt32"),
		nilInt64:   r.ReadNullableInt64("nilInt64"),
		nilFloat32: r.ReadNullableFloat32("nilFloat32"),
		nilFloat64: r.ReadNullableFloat64("nilFloat64"),
	}
}

func (s nullablesSerializer) Write(w pubserialization.CompactWriter, v interface{}) {
	vv := v.(nullables)
	w.WriteNullableBoolean("nilBool", vv.nilBool)
	w.WriteNullableInt8("nilInt8", vv.nilInt8)
	w.WriteNullableInt16("nilInt16", vv.nilInt16)
	w.WriteNullableInt32("nilInt32", vv.nilInt32)
	w.WriteNullableInt64("nilInt64", vv.nilInt64)
	w.WriteNullableFloat32("nilFloat32", vv.nilFloat32)
	w.WriteNullableFloat64("nilFloat64", vv.nilFloat64)
}

func nullableSerializerTest(t *testing.T) {
	var cfg pubserialization.Config
	cfg.Compact.SetSerializers(&nullablesSerializer{})
	ss := mustSerializationService(serialization.NewService(&cfg, nil))
	target := nullables{}
	data := mustData(ss.ToData(target))
	v := it.MustValue(ss.ToObject(data))
	require.Equal(t, target, v)
}
