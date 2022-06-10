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

package serialization_test

import (
	"math/big"
	"reflect"
	"time"

	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type NamedDTO struct {
	name  *string
	myint int32
}

type InnerDTO struct {
	bools            []bool
	bytes            []int8
	shorts           []int16
	ints             []int32
	longs            []int64
	floats           []float32
	doubles          []float64
	strings          []*string
	nn               []*NamedDTO
	bigDecimals      []*types.Decimal
	localTimes       []*types.LocalTime
	localDates       []*types.LocalDate
	localDateTimes   []*types.LocalDateTime
	offsetDateTimes  []*types.OffsetDateTime
	nullableBools    []*bool
	nullableBytes    []*int8
	nullableShorts   []*int16
	nullableIntegers []*int32
	nullableLongs    []*int64
	nullableFloats   []*float32
	nullableDoubles  []*float64
}

type MainDTO struct {
	b              int8
	boolean        bool
	s              int16
	i              int32
	l              int64
	f              float32
	d              float64
	str            *string
	p              *InnerDTO
	bigDecimal     *types.Decimal
	localTime      *types.LocalTime
	localDate      *types.LocalDate
	localDateTime  *types.LocalDateTime
	offsetDateTime *types.OffsetDateTime
	nullableB      *int8
	nullableBool   *bool
	nullableS      *int16
	nullableI      *int32
	nullableL      *int64
	nullableF      *float32
	nullableD      *float64
}

func NewInnerDTO() InnerDTO {
	now := time.Now()

	testStr := "test"

	nowLocalTime := types.LocalTime(time.Date(0, 1, 1, now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), time.Local))
	nowLocalTime2 := types.LocalTime(time.Date(0, 1, 1, now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), time.Local))

	nowLocalDate := types.LocalDate(time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local))
	nowLocalDate2 := types.LocalDate(time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local))

	nowLocalDateTime := types.LocalDateTime(time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), time.Local))
	nowOffsetDateTime := types.OffsetDateTime(time.Now())

	aBool := true
	aBool2 := false

	aSignedByte := int8(0)
	aSignedByte2 := int8(1)
	aSignedByte3 := int8(2)

	aShort := int16(3)
	aShort2 := int16(4)
	aShort3 := int16(5)

	anInt := int32(9)
	anInt2 := int32(8)
	anInt3 := int32(7)
	anInt4 := int32(6)

	aLong := int64(0)
	aLong2 := int64(1)
	aLong3 := int64(5)
	aLong4 := int64(7)
	aLong5 := int64(9)
	aLong6 := int64(11)

	aFloat := float32(0.6543)
	aFloat2 := float32(-3.56)
	aFloat3 := float32(45.67)

	aDouble := float64(456.456)
	aDouble2 := float64(789.789)
	aDouble3 := float64(321.321)

	bigDec1 := types.NewDecimal(big.NewInt(12345), 0)
	bigDec2 := types.NewDecimal(big.NewInt(123456), 0)

	nn := make([]*NamedDTO, 2)
	nameStr := "name"
	nameStr2 := "name"
	nn[0] = &NamedDTO{name: &nameStr, myint: 123}
	nn[1] = &NamedDTO{name: &nameStr2, myint: 123}

	return InnerDTO{
		bools: []bool{true, false}, bytes: []int8{0, 1, 2}, shorts: []int16{3, 4, 5},
		ints: []int32{9, 8, 7, 6}, longs: []int64{0, 1, 5, 7, 9, 11}, floats: []float32{0.6543, -3.56, 45.67},
		doubles: []float64{456.456, 789.789, 321.321}, strings: []*string{&testStr, nil}, nn: nn,
		bigDecimals: []*types.Decimal{&bigDec1, &bigDec2}, localTimes: []*types.LocalTime{&nowLocalTime, nil, &nowLocalTime2},
		localDates: []*types.LocalDate{&nowLocalDate, nil, &nowLocalDate2}, localDateTimes: []*types.LocalDateTime{&nowLocalDateTime, nil},
		offsetDateTimes: []*types.OffsetDateTime{&nowOffsetDateTime}, nullableBools: []*bool{&aBool, &aBool2, nil},
		nullableBytes: []*int8{&aSignedByte, &aSignedByte2, &aSignedByte3, nil}, nullableShorts: []*int16{&aShort, &aShort2, &aShort3, nil},
		nullableIntegers: []*int32{&anInt, &anInt2, &anInt3, &anInt4, nil}, nullableLongs: []*int64{&aLong, &aLong2, &aLong3, &aLong4, &aLong5, &aLong6},
		nullableFloats: []*float32{&aFloat, &aFloat2, &aFloat3}, nullableDoubles: []*float64{&aDouble, &aDouble2, &aDouble3},
	}
}

func NewMainDTO() MainDTO {
	now := time.Now()
	str := "this is main object created for testing!"

	inner := NewInnerDTO()

	bigDecimal := types.NewDecimal(big.NewInt(12312313), 0)
	localTime := types.LocalTime(time.Date(0, 1, 1, now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), time.Local))
	localDate := types.LocalDate(time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local))
	localDateTime := types.LocalDateTime(time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), time.Local))
	offsetDateTime := types.OffsetDateTime(time.Now())
	nullableB := int8(113)
	nullableBool := true
	nullableS := int16(-500)
	nullableI := int32(56789)
	nullableL := int64(-50992225)
	nullableF := float32(900.5678)
	nullableD := float64(-897543.3678909)

	return MainDTO{b: 113, boolean: true, s: -500, i: 56789, l: -50992225, f: 900.5678, d: -897543.3678909,
		str: &str, p: &inner, bigDecimal: &bigDecimal, localTime: &localTime, localDate: &localDate, localDateTime: &localDateTime,
		offsetDateTime: &offsetDateTime, nullableB: &nullableB, nullableBool: &nullableBool, nullableS: &nullableS, nullableI: &nullableI,
		nullableL: &nullableL, nullableF: &nullableF, nullableD: &nullableD,
	}
}

type MainDTOSerializer struct {
}

func (MainDTOSerializer) Type() reflect.Type {
	return reflect.TypeOf(MainDTO{})
}

func (MainDTOSerializer) TypeName() string {
	return "MainDTO"
}

func (MainDTOSerializer) Read(reader serialization.CompactReader) interface{} {
	boolean := reader.ReadBoolean("bool")
	b := reader.ReadInt8("b")
	s := reader.ReadInt16("s")
	i := reader.ReadInt32("i")
	l := reader.ReadInt64("l")
	f := reader.ReadFloat32("f")
	d := reader.ReadFloat64("d")
	str := reader.ReadString("str")
	p := reader.ReadCompact("p").(InnerDTO)
	bigDecimal := reader.ReadDecimal("bigDecimal")
	localTime := reader.ReadTime("localTime")
	localDate := reader.ReadDate("localDate")
	localDateTime := reader.ReadTimestamp("localDateTime")
	offsetDateTime := reader.ReadTimestampWithTimezone("offsetDateTime")
	nullableB := reader.ReadNullableInt8("nullableB")
	nullableBool := reader.ReadNullableBoolean("nullableBool")
	nullableS := reader.ReadNullableInt16("nullableS")
	nullableI := reader.ReadNullableInt32("nullableI")
	nullableL := reader.ReadNullableInt64("nullableL")
	nullableF := reader.ReadNullableFloat32("nullableF")
	nullableD := reader.ReadNullableFloat64("nullableD")

	return MainDTO{
		boolean: boolean, b: b, s: s, i: i, l: l, f: f, d: d, str: str, p: &p, bigDecimal: bigDecimal,
		localTime: localTime, localDate: localDate, localDateTime: localDateTime, offsetDateTime: offsetDateTime,
		nullableB: nullableB, nullableBool: nullableBool, nullableS: nullableS, nullableI: nullableI, nullableL: nullableL,
		nullableF: nullableF, nullableD: nullableD,
	}
}

func (MainDTOSerializer) Write(writer serialization.CompactWriter, value interface{}) {
	mainDTO, ok := value.(MainDTO)
	if !ok {
		panic("not a MainDTO")
	}
	writer.WriteBoolean("bool", mainDTO.boolean)
	writer.WriteInt8("b", mainDTO.b)
	writer.WriteInt16("s", mainDTO.s)
	writer.WriteInt32("i", mainDTO.i)
	writer.WriteInt64("l", mainDTO.l)
	writer.WriteFloat32("f", mainDTO.f)
	writer.WriteFloat64("d", mainDTO.d)
	writer.WriteString("str", mainDTO.str)
	writer.WriteCompact("p", mainDTO.p)
	writer.WriteDecimal("bigDecimal", mainDTO.bigDecimal)
	writer.WriteTime("localTime", mainDTO.localTime)
	writer.WriteDate("localDate", mainDTO.localDate)
	writer.WriteTimestamp("localDateTime", mainDTO.localDateTime)
	writer.WriteTimestampWithTimezone("offsetDateTime", mainDTO.offsetDateTime)
	writer.WriteNullableInt8("nullableB", mainDTO.nullableB)
	writer.WriteNullableBoolean("nullableBool", mainDTO.nullableBool)
	writer.WriteNullableInt16("nullableS", mainDTO.nullableS)
	writer.WriteNullableInt32("nullableI", mainDTO.nullableI)
	writer.WriteNullableInt64("nullableL", mainDTO.nullableL)
	writer.WriteNullableFloat32("nullableF", mainDTO.nullableF)
	writer.WriteNullableFloat64("nullableD", mainDTO.nullableD)
}

type CompactTest struct {
	boolean  bool
	b        int8
	short    int16
	i        int32
	long     int64
	float    float32
	double   float64
	booleans []bool
	bytes    []int8
	shorts   []int16
	ints     []int32
	longs    []int64
	floats   []float32
	doubles  []float64
}

func NewCompactTestObj() CompactTest {
	return CompactTest{boolean: true, b: 2, short: 4, i: 8, long: 4444, float: 8321.321, double: 41231.32, booleans: []bool{true, false},
		bytes: []int8{1, 2}, shorts: []int16{1, 4}, ints: []int32{1, 8}, longs: []int64{1, 4444}, floats: []float32{1, 8321.321}, doubles: []float64{41231.32, 2},
	}
}

type CompactTestWritePrimitiveReadNullableSerializer struct {
}

func (CompactTestWritePrimitiveReadNullableSerializer) Type() reflect.Type {
	return reflect.TypeOf(CompactTest{})
}

func (CompactTestWritePrimitiveReadNullableSerializer) TypeName() string {
	return "Test"
}

func (CompactTestWritePrimitiveReadNullableSerializer) Read(reader serialization.CompactReader) interface{} {
	boolean := reader.ReadNullableBoolean("boolean")
	b := reader.ReadNullableInt8("b")
	short := reader.ReadNullableInt16("short")
	i := reader.ReadNullableInt32("i")
	long := reader.ReadNullableInt64("long")
	float := reader.ReadNullableFloat32("float")
	double := reader.ReadNullableFloat64("double")
	nullableBooleans := reader.ReadArrayOfNullableBoolean("booleans")
	booleans := make([]bool, len(nullableBooleans))
	for i, b := range nullableBooleans {
		booleans[i] = *b
	}
	nullableBytes := reader.ReadArrayOfNullableInt8("bytes")
	bytes := make([]int8, len(nullableBytes))
	for i, b := range nullableBytes {
		bytes[i] = *b
	}
	nullableShorts := reader.ReadArrayOfNullableInt16("shorts")
	shorts := make([]int16, len(nullableShorts))
	for i, b := range nullableShorts {
		shorts[i] = *b
	}
	nullableInts := reader.ReadArrayOfNullableInt32("ints")
	ints := make([]int32, len(nullableInts))
	for i, b := range nullableInts {
		ints[i] = *b
	}
	nullableLongs := reader.ReadArrayOfNullableInt64("longs")
	longs := make([]int64, len(nullableLongs))
	for i, b := range nullableLongs {
		longs[i] = *b
	}
	nullableFloats := reader.ReadArrayOfNullableFloat32("floats")
	floats := make([]float32, len(nullableFloats))
	for i, b := range nullableFloats {
		floats[i] = *b
	}
	nullableDoubles := reader.ReadArrayOfNullableFloat64("doubles")
	doubles := make([]float64, len(nullableDoubles))
	for i, b := range nullableDoubles {
		doubles[i] = *b
	}

	return CompactTest{boolean: *boolean, b: *b, short: *short, i: *i, long: *long,
		float: *float, double: *double, booleans: booleans, bytes: bytes, shorts: shorts,
		ints: ints, longs: longs, floats: floats, doubles: doubles,
	}
}

func (CompactTestWritePrimitiveReadNullableSerializer) Write(writer serialization.CompactWriter, value interface{}) {
	test, ok := value.(CompactTest)
	if !ok {
		panic("not a Test")
	}
	writer.WriteBoolean("boolean", test.boolean)
	writer.WriteInt8("b", test.b)
	writer.WriteInt16("short", test.short)
	writer.WriteInt32("i", test.i)
	writer.WriteInt64("long", test.long)
	writer.WriteFloat32("float", test.float)
	writer.WriteFloat64("double", test.double)
	writer.WriteArrayOfBoolean("booleans", test.booleans)
	writer.WriteArrayOfInt8("bytes", test.bytes)
	writer.WriteArrayOfInt16("shorts", test.shorts)
	writer.WriteArrayOfInt32("ints", test.ints)
	writer.WriteArrayOfInt64("longs", test.longs)
	writer.WriteArrayOfFloat32("floats", test.floats)
	writer.WriteArrayOfFloat64("doubles", test.doubles)
}

type CompactTestWriteNullableReadPrimitiveSerializer struct {
}

func (CompactTestWriteNullableReadPrimitiveSerializer) Type() reflect.Type {
	return reflect.TypeOf(CompactTest{})
}

func (CompactTestWriteNullableReadPrimitiveSerializer) TypeName() string {
	return "Test"
}

func (CompactTestWriteNullableReadPrimitiveSerializer) Read(reader serialization.CompactReader) interface{} {
	boolean := reader.ReadBoolean("boolean")
	b := reader.ReadInt8("b")
	short := reader.ReadInt16("short")
	i := reader.ReadInt32("i")
	long := reader.ReadInt64("long")
	float := reader.ReadFloat32("float")
	double := reader.ReadFloat64("double")
	booleans := reader.ReadArrayOfBoolean("booleans")
	bytes := reader.ReadArrayOfInt8("bytes")
	shorts := reader.ReadArrayOfInt16("shorts")
	ints := reader.ReadArrayOfInt32("ints")
	longs := reader.ReadArrayOfInt64("longs")
	floats := reader.ReadArrayOfFloat32("floats")
	doubles := reader.ReadArrayOfFloat64("doubles")

	return CompactTest{boolean: boolean, b: b, short: short, i: i, long: long,
		float: float, double: double, booleans: booleans, bytes: bytes, shorts: shorts,
		ints: ints, longs: longs, floats: floats, doubles: doubles,
	}
}

func (CompactTestWriteNullableReadPrimitiveSerializer) Write(writer serialization.CompactWriter, value interface{}) {
	test, ok := value.(CompactTest)
	if !ok {
		panic("not a Test")
	}
	writer.WriteNullableBoolean("boolean", &test.boolean)
	writer.WriteNullableInt8("b", &test.b)
	writer.WriteNullableInt16("short", &test.short)
	writer.WriteNullableInt32("i", &test.i)
	writer.WriteNullableInt64("long", &test.long)
	writer.WriteNullableFloat32("float", &test.float)
	writer.WriteNullableFloat64("double", &test.double)
	writer.WriteArrayOfBoolean("booleans", test.booleans)
	nullableBytes := make([]*int8, len(test.bytes))
	for i, b := range test.bytes {
		value := b
		nullableBytes[i] = &value
	}
	writer.WriteArrayOfNullableInt8("bytes", nullableBytes)
	nullableShorts := make([]*int16, len(test.shorts))
	for i, s := range test.shorts {
		value := s
		nullableShorts[i] = &value
	}
	writer.WriteArrayOfNullableInt16("shorts", nullableShorts)
	nullableInts := make([]*int32, len(test.ints))
	for i, v := range test.ints {
		value := v
		nullableInts[i] = &value
	}
	writer.WriteArrayOfNullableInt32("ints", nullableInts)
	nullableLongs := make([]*int64, len(test.longs))
	for i, v := range test.longs {
		value := v
		nullableLongs[i] = &value
	}
	writer.WriteArrayOfNullableInt64("longs", nullableLongs)
	nullableFloats := make([]*float32, len(test.floats))
	for i, v := range test.floats {
		value := v
		nullableFloats[i] = &value
	}
	writer.WriteArrayOfNullableFloat32("floats", nullableFloats)
	nullableDoubles := make([]*float64, len(test.doubles))
	for i, v := range test.doubles {
		value := v
		nullableDoubles[i] = &value
	}
	writer.WriteArrayOfNullableFloat64("doubles", nullableDoubles)
}

type CompactTestWriteNullReadPrimitiveSerializer struct {
}

func (CompactTestWriteNullReadPrimitiveSerializer) Type() reflect.Type {
	return reflect.TypeOf(CompactTest{})
}

func (CompactTestWriteNullReadPrimitiveSerializer) TypeName() string {
	return "Test"
}

func (CompactTestWriteNullReadPrimitiveSerializer) Read(reader serialization.CompactReader) interface{} {
	boolean := reader.ReadBoolean("boolean")
	b := reader.ReadInt8("b")
	short := reader.ReadInt16("short")
	i := reader.ReadInt32("i")
	long := reader.ReadInt64("long")
	float := reader.ReadFloat32("float")
	double := reader.ReadFloat64("double")
	booleans := reader.ReadArrayOfBoolean("booleans")
	bytes := reader.ReadArrayOfInt8("bytes")
	shorts := reader.ReadArrayOfInt16("shorts")
	ints := reader.ReadArrayOfInt32("ints")
	longs := reader.ReadArrayOfInt64("longs")
	floats := reader.ReadArrayOfFloat32("floats")
	doubles := reader.ReadArrayOfFloat64("doubles")

	return CompactTest{boolean: boolean, b: b, short: short, i: i, long: long,
		float: float, double: double, booleans: booleans, bytes: bytes, shorts: shorts,
		ints: ints, longs: longs, floats: floats, doubles: doubles,
	}
}

func (CompactTestWriteNullReadPrimitiveSerializer) Write(writer serialization.CompactWriter, value interface{}) {
	test, ok := value.(CompactTest)
	if !ok {
		panic("not a Test")
	}
	writer.WriteNullableBoolean("boolean", nil)
	writer.WriteNullableInt8("b", nil)
	writer.WriteNullableInt16("short", nil)
	writer.WriteNullableInt32("i", nil)
	writer.WriteNullableInt64("long", nil)
	writer.WriteNullableFloat32("float", nil)
	writer.WriteNullableFloat64("double", nil)
	writer.WriteArrayOfBoolean("booleans", nil)
	nullableBytes := make([]*int8, len(test.bytes)+1)
	for i, b := range test.bytes {
		value := b
		nullableBytes[i] = &value
	}
	nullableBytes = append(nullableBytes, nil)
	writer.WriteArrayOfNullableInt8("bytes", nullableBytes)
	nullableShorts := make([]*int16, len(test.shorts)+1)
	for i, s := range test.shorts {
		value := s
		nullableShorts[i] = &value
	}
	nullableShorts = append(nullableShorts, nil)
	writer.WriteArrayOfNullableInt16("shorts", nullableShorts)
	nullableInts := make([]*int32, len(test.ints)+1)
	for i, v := range test.ints {
		value := v
		nullableInts[i] = &value
	}
	nullableInts = append(nullableInts, nil)
	writer.WriteArrayOfNullableInt32("ints", nullableInts)
	nullableLongs := make([]*int64, len(test.longs)+1)
	for i, v := range test.longs {
		value := v
		nullableLongs[i] = &value
	}
	nullableLongs = append(nullableLongs, nil)
	writer.WriteArrayOfNullableInt64("longs", nullableLongs)
	nullableFloats := make([]*float32, len(test.floats)+1)
	for i, v := range test.floats {
		value := v
		nullableFloats[i] = &value
	}
	nullableFloats = append(nullableFloats, nil)
	writer.WriteArrayOfNullableFloat32("floats", nullableFloats)
	nullableDoubles := make([]*float64, len(test.doubles)+1)
	for i, v := range test.doubles {
		value := v
		nullableDoubles[i] = &value
	}
	nullableDoubles = append(nullableDoubles, nil)
	writer.WriteArrayOfNullableFloat64("doubles", nullableDoubles)
}

type InnerDTOSerializer struct {
}

func (InnerDTOSerializer) Type() reflect.Type {
	return reflect.TypeOf(InnerDTO{})
}

func (InnerDTOSerializer) TypeName() string {
	return "InnerDTO"
}

func (InnerDTOSerializer) Read(reader serialization.CompactReader) interface{} {
	bools := reader.ReadArrayOfBoolean("bools")
	bytes := reader.ReadArrayOfInt8("bytes")
	shorts := reader.ReadArrayOfInt16("shorts")
	ints := reader.ReadArrayOfInt32("ints")
	longs := reader.ReadArrayOfInt64("longs")
	floats := reader.ReadArrayOfFloat32("floats")
	doubles := reader.ReadArrayOfFloat64("doubles")
	strings := reader.ReadArrayOfString("strings")
	nn := reader.ReadArrayOfCompact("nn")
	namedDTOs := make([]*NamedDTO, len(nn))
	for i, n := range nn {
		if n == nil {
			namedDTOs[i] = nil
		} else {
			np := n.(NamedDTO)
			namedDTOs[i] = &np
		}
	}
	bigDecimals := reader.ReadArrayOfDecimal("bigDecimals")
	localTimes := reader.ReadArrayOfTime("localTimes")
	localDates := reader.ReadArrayOfDate("localDates")
	localDateTimes := reader.ReadArrayOfTimestamp("localDateTimes")
	offsetDateTimes := reader.ReadArrayOfTimestampWithTimezone("offsetDateTimes")
	nullableBools := reader.ReadArrayOfNullableBoolean("nullableBools")
	nullableBytes := reader.ReadArrayOfNullableInt8("nullableBytes")
	nullableShorts := reader.ReadArrayOfNullableInt16("nullableShorts")
	nullableIntegers := reader.ReadArrayOfNullableInt32("nullableIntegers")
	nullableLongs := reader.ReadArrayOfNullableInt64("nullableLongs")
	nullableFloats := reader.ReadArrayOfNullableFloat32("nullableFloats")
	nullableDoubles := reader.ReadArrayOfNullableFloat64("nullableDoubles")
	return InnerDTO{
		bools: bools, bytes: bytes, shorts: shorts, ints: ints, longs: longs, floats: floats, doubles: doubles,
		strings: strings, nn: namedDTOs, bigDecimals: bigDecimals, localTimes: localTimes, localDates: localDates,
		localDateTimes: localDateTimes, offsetDateTimes: offsetDateTimes, nullableBools: nullableBools,
		nullableBytes: nullableBytes, nullableShorts: nullableShorts, nullableIntegers: nullableIntegers,
		nullableLongs: nullableLongs, nullableFloats: nullableFloats, nullableDoubles: nullableDoubles,
	}
}

func (InnerDTOSerializer) Write(writer serialization.CompactWriter, value interface{}) {
	innerDTO, ok := value.(InnerDTO)
	if !ok {
		panic("not a InnerDTO")
	}

	writer.WriteArrayOfBoolean("bools", innerDTO.bools)
	writer.WriteArrayOfInt8("bytes", innerDTO.bytes)
	writer.WriteArrayOfInt16("shorts", innerDTO.shorts)
	writer.WriteArrayOfInt32("ints", innerDTO.ints)
	writer.WriteArrayOfInt64("longs", innerDTO.longs)
	writer.WriteArrayOfFloat32("floats", innerDTO.floats)
	writer.WriteArrayOfFloat64("doubles", innerDTO.doubles)
	writer.WriteArrayOfString("strings", innerDTO.strings)
	interfaceValues := make([]interface{}, len(innerDTO.nn))
	for i, n := range innerDTO.nn {
		interfaceValues[i] = n
	}
	writer.WriteArrayOfCompact("nn", interfaceValues)
	writer.WriteArrayOfDecimal("bigDecimals", innerDTO.bigDecimals)
	writer.WriteArrayOfTime("localTimes", innerDTO.localTimes)
	writer.WriteArrayOfDate("localDates", innerDTO.localDates)
	writer.WriteArrayOfTimestamp("localDateTimes", innerDTO.localDateTimes)
	writer.WriteArrayOfTimestampWithTimezone("offsetDateTimes", innerDTO.offsetDateTimes)
	writer.WriteArrayOfNullableBoolean("nullableBools", innerDTO.nullableBools)
	writer.WriteArrayOfNullableInt8("nullableBytes", innerDTO.nullableBytes)
	writer.WriteArrayOfNullableInt16("nullableShorts", innerDTO.nullableShorts)
	writer.WriteArrayOfNullableInt32("nullableIntegers", innerDTO.nullableIntegers)
	writer.WriteArrayOfNullableInt64("nullableLongs", innerDTO.nullableLongs)
	writer.WriteArrayOfNullableFloat32("nullableFloats", innerDTO.nullableFloats)
	writer.WriteArrayOfNullableFloat64("nullableDoubles", innerDTO.nullableDoubles)
}

type NamedDTOSerializer struct {
}

func (NamedDTOSerializer) Type() reflect.Type {
	return reflect.TypeOf(NamedDTO{})
}

func (NamedDTOSerializer) TypeName() string {
	return "NamedDTO"
}

func (NamedDTOSerializer) Read(reader serialization.CompactReader) interface{} {
	return NamedDTO{name: reader.ReadString("name"), myint: reader.ReadInt32("myint")}
}

func (NamedDTOSerializer) Write(writer serialization.CompactWriter, value interface{}) {
	namedDTO, ok := value.(NamedDTO)
	if !ok {
		panic("not a NamedDTO")
	}

	writer.WriteString("name", namedDTO.name)
	writer.WriteInt32("myint", namedDTO.myint)
}

type EmployeeDTO struct {
	age int32
	id  int64
}

type EmployeeDTOCompactSerializer struct{}

func (EmployeeDTOCompactSerializer) Type() reflect.Type {
	return reflect.TypeOf(EmployeeDTO{})
}

func (s EmployeeDTOCompactSerializer) TypeName() string {
	return "employee"
}

func (s EmployeeDTOCompactSerializer) Read(reader serialization.CompactReader) interface{} {
	return EmployeeDTO{
		age: reader.ReadInt32("age"),
		id:  reader.ReadInt64("id"),
	}
}

func (s EmployeeDTOCompactSerializer) Write(writer serialization.CompactWriter, value interface{}) {
	c, ok := value.(EmployeeDTO)
	if !ok {
		panic("not an employeeDTO")
	}
	writer.WriteInt32("age", c.age)
	writer.WriteInt64("id", c.id)
}
