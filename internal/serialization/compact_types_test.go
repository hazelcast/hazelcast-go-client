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
	"math/big"
	"reflect"
	"time"

	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// ported from: com.hazelcast.internal.serialization.impl.compact.CompactTestUtil

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

type BitsDTO struct {
	booleans []bool
	id       int32
	a        bool
	b        bool
	e        bool
	f        bool
	g        bool
	h        bool
	c        bool
	d        bool
}

type BitsDTOSerializer struct {
}

func (BitsDTOSerializer) Type() reflect.Type {
	return reflect.TypeOf(BitsDTO{})
}

func (BitsDTOSerializer) TypeName() string {
	return "BitsDTO"
}

func (BitsDTOSerializer) Read(reader serialization.CompactReader) interface{} {
	a := reader.ReadBoolean("a")
	b := reader.ReadBoolean("b")
	c := reader.ReadBoolean("c")
	d := reader.ReadBoolean("d")
	e := reader.ReadBoolean("e")
	f := reader.ReadBoolean("f")
	g := reader.ReadBoolean("g")
	h := reader.ReadBoolean("h")
	id := reader.ReadInt32("id")
	booleans := reader.ReadArrayOfBoolean("booleans")
	return BitsDTO{a: a, b: b, c: c, d: d, e: e, f: f, g: g, h: h, id: id, booleans: booleans}
}

func (BitsDTOSerializer) Write(writer serialization.CompactWriter, value interface{}) {
	bitsDTO, ok := value.(BitsDTO)
	if !ok {
		panic("not a BitsDTO")
	}
	writer.WriteBoolean("a", bitsDTO.a)
	writer.WriteBoolean("b", bitsDTO.b)
	writer.WriteBoolean("c", bitsDTO.c)
	writer.WriteBoolean("d", bitsDTO.d)
	writer.WriteBoolean("e", bitsDTO.e)
	writer.WriteBoolean("f", bitsDTO.f)
	writer.WriteBoolean("g", bitsDTO.g)
	writer.WriteBoolean("h", bitsDTO.h)
	writer.WriteInt32("id", bitsDTO.id)
	writer.WriteArrayOfBoolean("booleans", bitsDTO.booleans)
}

type MainDTO struct {
	offsetDateTime *types.OffsetDateTime
	nullableF      *float32
	nullableL      *int64
	nullableI      *int32
	nullableS      *int16
	nullableBool   *bool
	nullableB      *int8
	str            *string
	p              *InnerDTO
	bigDecimal     *types.Decimal
	localTime      *types.LocalTime
	localDate      *types.LocalDate
	localDateTime  *types.LocalDateTime
	nullableD      *float64
	d              float64
	l              int64
	f              float32
	i              int32
	s              int16
	boolean        bool
	b              int8
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
	bools := []bool{true, false}
	bytes := []int8{0, 1, 2}
	shorts := []int16{3, 4, 5}
	ints := []int32{9, 8, 7, 6}
	longs := []int64{0, 1, 5, 7, 9, 11}
	floats := []float32{0.6543, -3.56, 45.67}
	doubles := []float64{456.456, 789.789, 321.321}
	bigDec1 := types.NewDecimal(big.NewInt(12345), 0)
	bigDec2 := types.NewDecimal(big.NewInt(123456), 0)
	nn := make([]*NamedDTO, 2)
	nameStr := "name"
	nameStr2 := "name"
	nn[0] = &NamedDTO{name: &nameStr, myint: 123}
	nn[1] = &NamedDTO{name: &nameStr2, myint: 123}
	return InnerDTO{
		bools:            bools,
		bytes:            bytes,
		shorts:           shorts,
		ints:             ints,
		longs:            longs,
		floats:           floats,
		doubles:          doubles,
		strings:          []*string{&testStr, nil},
		nn:               nn,
		bigDecimals:      []*types.Decimal{&bigDec1, &bigDec2},
		localTimes:       []*types.LocalTime{&nowLocalTime, nil, &nowLocalTime2},
		localDates:       []*types.LocalDate{&nowLocalDate, nil, &nowLocalDate2},
		localDateTimes:   []*types.LocalDateTime{&nowLocalDateTime, nil},
		offsetDateTimes:  []*types.OffsetDateTime{&nowOffsetDateTime},
		nullableBools:    nullableBoolSlice(bools...),
		nullableBytes:    nullableInt8Slice(bytes...),
		nullableShorts:   nullableInt16Slice(shorts...),
		nullableIntegers: nullableInt32Slice(ints...),
		nullableLongs:    nullableInt64Slice(longs...),
		nullableFloats:   nullableFloat32Slice(floats...),
		nullableDoubles:  nullableFloat64Slice(doubles...),
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

func readMainDTO(reader serialization.CompactReader) MainDTO {
	var boolean bool
	if reader.GetFieldKind("bool") == serialization.FieldKindNotAvailable {
		boolean = false
	} else {
		boolean = reader.ReadBoolean("bool")
	}
	var b int8
	if reader.GetFieldKind("b") == serialization.FieldKindNotAvailable {
		b = 1
	} else {
		b = reader.ReadInt8("b")
	}
	var s int16
	if reader.GetFieldKind("s") == serialization.FieldKindNotAvailable {
		s = 1
	} else {
		s = reader.ReadInt16("s")
	}
	var i int32
	if reader.GetFieldKind("i") == serialization.FieldKindNotAvailable {
		i = 1
	} else {
		i = reader.ReadInt32("i")
	}
	var l int64
	if reader.GetFieldKind("l") == serialization.FieldKindNotAvailable {
		l = 1
	} else {
		l = reader.ReadInt64("l")
	}
	var f float32
	if reader.GetFieldKind("f") == serialization.FieldKindNotAvailable {
		f = 1
	} else {
		f = reader.ReadFloat32("f")
	}
	var d float64
	if reader.GetFieldKind("d") == serialization.FieldKindNotAvailable {
		d = 1
	} else {
		d = reader.ReadFloat64("d")
	}
	var str *string
	defaultStr := "NA"
	if reader.GetFieldKind("str") == serialization.FieldKindNotAvailable {
		str = &defaultStr
	} else {
		str = reader.ReadString("str")
	}
	var p *InnerDTO
	if reader.GetFieldKind("p") == serialization.FieldKindNotAvailable {
		p = nil
	} else {
		compact := reader.ReadCompact("p")
		if compact == nil {
			p = nil
		} else {
			innerDTO := compact.(InnerDTO)
			p = &innerDTO
		}
	}
	var bigDecimal *types.Decimal
	defaultDecimal := types.NewDecimal(big.NewInt(1), 0)
	if reader.GetFieldKind("bigDecimal") == serialization.FieldKindNotAvailable {
		bigDecimal = &defaultDecimal
	} else {
		bigDecimal = reader.ReadDecimal("bigDecimal")
	}
	var localTime *types.LocalTime
	defaultLocalTime := types.LocalTime(time.Date(0, 0, 0, 1, 1, 1, 0, time.Local))
	if reader.GetFieldKind("localTime") == serialization.FieldKindNotAvailable {
		localTime = &defaultLocalTime
	} else {
		localTime = reader.ReadTime("localTime")
	}
	var localDate *types.LocalDate
	defaultlocalDate := types.LocalDate(time.Date(1, 1, 1, 0, 0, 0, 0, time.Local))
	if reader.GetFieldKind("localDate") == serialization.FieldKindNotAvailable {
		localDate = &defaultlocalDate
	} else {
		localDate = reader.ReadDate("localDate")
	}
	var localDateTime *types.LocalDateTime
	defaultlocalDateTime := types.LocalDateTime(time.Date(1, 1, 1, 1, 1, 1, 0, time.Local))
	if reader.GetFieldKind("localDateTime") == serialization.FieldKindNotAvailable {
		localDateTime = &defaultlocalDateTime
	} else {
		localDateTime = reader.ReadTimestamp("localDateTime")
	}
	var offsetDateTime *types.OffsetDateTime
	defaultOffsetDateTime := types.OffsetDateTime(time.Date(1, 1, 1, 1, 1, 1, 1, time.FixedZone("", 3600)))
	if reader.GetFieldKind("offsetDateTime") == serialization.FieldKindNotAvailable {
		offsetDateTime = &defaultOffsetDateTime
	} else {
		offsetDateTime = reader.ReadTimestampWithTimezone("offsetDateTime")
	}
	var nullableB *int8
	defaultNullableB := int8(1)
	if reader.GetFieldKind("nullableB") == serialization.FieldKindNotAvailable {
		nullableB = &defaultNullableB
	} else {
		nullableB = reader.ReadNullableInt8("nullableB")
	}
	var nullableBool *bool
	defaultNullableBool := false
	if reader.GetFieldKind("nullableBool") == serialization.FieldKindNotAvailable {
		nullableBool = &defaultNullableBool
	} else {
		nullableBool = reader.ReadNullableBoolean("nullableBool")
	}
	var nullableS *int16
	defaultNullableS := int16(1)
	if reader.GetFieldKind("nullableS") == serialization.FieldKindNotAvailable {
		nullableS = &defaultNullableS
	} else {
		nullableS = reader.ReadNullableInt16("nullableS")
	}
	var nullableI *int32
	defaultNullableI := int32(1)
	if reader.GetFieldKind("nullableI") == serialization.FieldKindNotAvailable {
		nullableI = &defaultNullableI
	} else {
		nullableI = reader.ReadNullableInt32("nullableI")
	}
	var nullableL *int64
	defaultNullableL := int64(1)
	if reader.GetFieldKind("nullableL") == serialization.FieldKindNotAvailable {
		nullableL = &defaultNullableL
	} else {
		nullableL = reader.ReadNullableInt64("nullableL")
	}
	var nullableF *float32
	defaultNullableF := float32(1)
	if reader.GetFieldKind("nullableF") == serialization.FieldKindNotAvailable {
		nullableF = &defaultNullableF
	} else {
		nullableF = reader.ReadNullableFloat32("nullableF")
	}
	var nullableD *float64
	defaultNullableD := float64(1)
	if reader.GetFieldKind("nullableD") == serialization.FieldKindNotAvailable {
		nullableD = &defaultNullableD
	} else {
		nullableD = reader.ReadNullableFloat64("nullableD")
	}

	return MainDTO{
		boolean: boolean, b: b, s: s, i: i, l: l, f: f, d: d, str: str, p: p, bigDecimal: bigDecimal,
		localTime: localTime, localDate: localDate, localDateTime: localDateTime, offsetDateTime: offsetDateTime,
		nullableB: nullableB, nullableBool: nullableBool, nullableS: nullableS, nullableI: nullableI, nullableL: nullableL,
		nullableF: nullableF, nullableD: nullableD,
	}
}

func readInnerDTO(reader serialization.CompactReader) InnerDTO {
	var bools []bool
	if reader.GetFieldKind("bools") == serialization.FieldKindNotAvailable {
		bools = make([]bool, 0)
	} else {
		bools = reader.ReadArrayOfBoolean("bools")
	}
	var bytes []int8
	if reader.GetFieldKind("bytes") == serialization.FieldKindNotAvailable {
		bytes = make([]int8, 0)
	} else {
		bytes = reader.ReadArrayOfInt8("bytes")
	}
	var shorts []int16
	if reader.GetFieldKind("shorts") == serialization.FieldKindNotAvailable {
		shorts = make([]int16, 0)
	} else {
		shorts = reader.ReadArrayOfInt16("shorts")
	}
	var ints []int32
	if reader.GetFieldKind("ints") == serialization.FieldKindNotAvailable {
		ints = make([]int32, 0)
	} else {
		ints = reader.ReadArrayOfInt32("ints")
	}
	var longs []int64
	if reader.GetFieldKind("longs") == serialization.FieldKindNotAvailable {
		longs = make([]int64, 0)
	} else {
		longs = reader.ReadArrayOfInt64("longs")
	}
	var floats []float32
	if reader.GetFieldKind("floats") == serialization.FieldKindNotAvailable {
		floats = make([]float32, 0)
	} else {
		floats = reader.ReadArrayOfFloat32("floats")
	}
	var doubles []float64
	if reader.GetFieldKind("doubles") == serialization.FieldKindNotAvailable {
		doubles = make([]float64, 0)
	} else {
		doubles = reader.ReadArrayOfFloat64("doubles")
	}
	var strings []*string
	if reader.GetFieldKind("strings") == serialization.FieldKindNotAvailable {
		strings = make([]*string, 0)
	} else {
		strings = reader.ReadArrayOfString("strings")
	}
	var namedDTOs []*NamedDTO
	if reader.GetFieldKind("nn") == serialization.FieldKindNotAvailable {
		namedDTOs = make([]*NamedDTO, 0)
	} else {
		nn := reader.ReadArrayOfCompact("nn")
		namedDTOs = make([]*NamedDTO, len(nn))
		for i, n := range nn {
			if n == nil {
				namedDTOs[i] = nil
			} else {
				np := n.(NamedDTO)
				namedDTOs[i] = &np
			}
		}
	}
	var bigDecimals []*types.Decimal
	if reader.GetFieldKind("bigDecimals") == serialization.FieldKindNotAvailable {
		bigDecimals = make([]*types.Decimal, 0)
	} else {
		bigDecimals = reader.ReadArrayOfDecimal("bigDecimals")
	}
	var localTimes []*types.LocalTime
	if reader.GetFieldKind("localTimes") == serialization.FieldKindNotAvailable {
		localTimes = make([]*types.LocalTime, 0)
	} else {
		localTimes = reader.ReadArrayOfTime("localTimes")
	}
	var localDates []*types.LocalDate
	if reader.GetFieldKind("localDates") == serialization.FieldKindNotAvailable {
		localDates = make([]*types.LocalDate, 0)
	} else {
		localDates = reader.ReadArrayOfDate("localDates")
	}
	var localDateTimes []*types.LocalDateTime
	if reader.GetFieldKind("localDateTimes") == serialization.FieldKindNotAvailable {
		localDateTimes = make([]*types.LocalDateTime, 0)
	} else {
		localDateTimes = reader.ReadArrayOfTimestamp("localDateTimes")
	}
	var offsetDateTimes []*types.OffsetDateTime
	if reader.GetFieldKind("offsetDateTimes") == serialization.FieldKindNotAvailable {
		offsetDateTimes = make([]*types.OffsetDateTime, 0)
	} else {
		offsetDateTimes = reader.ReadArrayOfTimestampWithTimezone("offsetDateTimes")
	}
	var nullableBools []*bool
	if reader.GetFieldKind("nullableBools") == serialization.FieldKindNotAvailable {
		nullableBools = make([]*bool, 0)
	} else {
		nullableBools = reader.ReadArrayOfNullableBoolean("nullableBools")
	}
	var nullableBytes []*int8
	if reader.GetFieldKind("nullableBytes") == serialization.FieldKindNotAvailable {
		nullableBytes = make([]*int8, 0)
	} else {
		nullableBytes = reader.ReadArrayOfNullableInt8("nullableBytes")
	}
	var nullableShorts []*int16
	if reader.GetFieldKind("nullableShorts") == serialization.FieldKindNotAvailable {
		nullableShorts = make([]*int16, 0)
	} else {
		nullableShorts = reader.ReadArrayOfNullableInt16("nullableShorts")
	}
	var nullableIntegers []*int32
	if reader.GetFieldKind("nullableIntegers") == serialization.FieldKindNotAvailable {
		nullableIntegers = make([]*int32, 0)
	} else {
		nullableIntegers = reader.ReadArrayOfNullableInt32("nullableIntegers")
	}
	var nullableLongs []*int64
	if reader.GetFieldKind("nullableLongs") == serialization.FieldKindNotAvailable {
		nullableLongs = make([]*int64, 0)
	} else {
		nullableLongs = reader.ReadArrayOfNullableInt64("nullableLongs")
	}
	var nullableFloats []*float32
	if reader.GetFieldKind("nullableFloats") == serialization.FieldKindNotAvailable {
		nullableFloats = make([]*float32, 0)
	} else {
		nullableFloats = reader.ReadArrayOfNullableFloat32("nullableFloats")
	}
	var nullableDoubles []*float64
	if reader.GetFieldKind("nullableDoubles") == serialization.FieldKindNotAvailable {
		nullableDoubles = make([]*float64, 0)
	} else {
		nullableDoubles = reader.ReadArrayOfNullableFloat64("nullableDoubles")
	}
	return InnerDTO{
		bools: bools, bytes: bytes, shorts: shorts, ints: ints, longs: longs, floats: floats, doubles: doubles,
		strings: strings, nn: namedDTOs, bigDecimals: bigDecimals, localTimes: localTimes, localDates: localDates,
		localDateTimes: localDateTimes, offsetDateTimes: offsetDateTimes, nullableBools: nullableBools,
		nullableBytes: nullableBytes, nullableShorts: nullableShorts, nullableIntegers: nullableIntegers,
		nullableLongs: nullableLongs, nullableFloats: nullableFloats, nullableDoubles: nullableDoubles,
	}
}

type NoWriteMainDTOSerializer struct {
}

func (NoWriteMainDTOSerializer) Type() reflect.Type {
	return reflect.TypeOf(MainDTO{})
}

func (NoWriteMainDTOSerializer) TypeName() string {
	return "MainDTO"
}

func (NoWriteMainDTOSerializer) Read(reader serialization.CompactReader) interface{} {
	return readMainDTO(reader)
}

func (NoWriteMainDTOSerializer) Write(writer serialization.CompactWriter, value interface{}) {
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
	return readMainDTO(reader)
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
	booleans []bool
	floats   []float32
	longs    []int64
	ints     []int32
	shorts   []int16
	bytes    []int8
	doubles  []float64
	double   float64
	long     int64
	float    float32
	i        int32
	short    int16
	b        int8
	boolean  bool
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
	return readInnerDTO(reader)
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

type NoWriteInnerDTOSerializer struct {
}

func (NoWriteInnerDTOSerializer) Type() reflect.Type {
	return reflect.TypeOf(InnerDTO{})
}

func (NoWriteInnerDTOSerializer) TypeName() string {
	return "InnerDTO"
}

func (NoWriteInnerDTOSerializer) Read(reader serialization.CompactReader) interface{} {
	return readInnerDTO(reader)
}

func (NoWriteInnerDTOSerializer) Write(writer serialization.CompactWriter, value interface{}) {
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

type EmployerDTO struct {
	name           *string
	hiringStatus   *string
	singleEmployee *EmployeeDTO
	ids            []int64
	otherEmployees []*EmployeeDTO
	zcode          int32
}

type EmployerDTOCompactSerializer struct{}

func (EmployerDTOCompactSerializer) Type() reflect.Type {
	return reflect.TypeOf(EmployerDTO{})
}

func (s EmployerDTOCompactSerializer) TypeName() string {
	return "EmployerDTO"
}

func (s EmployerDTOCompactSerializer) Read(reader serialization.CompactReader) interface{} {
	singleEmployee := reader.ReadCompact("singleEmployee").(EmployeeDTO)
	var otherEmployees []*EmployeeDTO
	readOtherEmployees := reader.ReadArrayOfCompact("otherEmployees")
	for _, v := range readOtherEmployees {
		employeeDTO := v.(EmployeeDTO)
		otherEmployees = append(otherEmployees, &employeeDTO)
	}
	return EmployerDTO{
		name:           reader.ReadString("name"),
		zcode:          reader.ReadInt32("zcode"),
		ids:            reader.ReadArrayOfInt64("ids"),
		hiringStatus:   reader.ReadString("hiringStatus"),
		singleEmployee: &singleEmployee,
		otherEmployees: otherEmployees,
	}
}

func (s EmployerDTOCompactSerializer) Write(writer serialization.CompactWriter, value interface{}) {
	c, ok := value.(EmployerDTO)
	if !ok {
		panic("not an EmployerDTO")
	}
	writer.WriteString("name", c.name)
	writer.WriteInt32("zcode", c.zcode)
	writer.WriteArrayOfInt64("ids", c.ids)
	writer.WriteString("hiringStatus", c.hiringStatus)
	writer.WriteCompact("singleEmployee", c.singleEmployee)
	var otherEmployees []interface{}
	for _, v := range c.otherEmployees {
		otherEmployees = append(otherEmployees, v)
	}
	writer.WriteArrayOfCompact("otherEmployees", otherEmployees)
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
	return "EmployeeDTO"
}

func (s EmployeeDTOCompactSerializer) Read(reader serialization.CompactReader) interface{} {
	var age int32
	if reader.GetFieldKind("age") == serialization.FieldKindNotAvailable {
		age = 0
	} else {
		age = reader.ReadInt32("age")
	}
	var id int64
	if reader.GetFieldKind("id") == serialization.FieldKindNotAvailable {
		id = 0
	} else {
		id = reader.ReadInt64("id")
	}
	return EmployeeDTO{
		age: age,
		id:  id,
	}
}

func (s EmployeeDTOCompactSerializer) Write(writer serialization.CompactWriter, value interface{}) {
	c, ok := value.(EmployeeDTO)
	if !ok {
		panic("not an EmployeeDTO")
	}
	writer.WriteInt32("age", c.age)
	writer.WriteInt64("id", c.id)
}

type EmployeeDTOCompactSerializerV2 struct{}

func (EmployeeDTOCompactSerializerV2) Type() reflect.Type {
	return reflect.TypeOf(EmployeeDTO{})
}

func (s EmployeeDTOCompactSerializerV2) TypeName() string {
	return "EmployeeDTO"
}

func (s EmployeeDTOCompactSerializerV2) Read(reader serialization.CompactReader) interface{} {
	return EmployeeDTO{
		age: reader.ReadInt32("age"),
		id:  reader.ReadInt64("id"),
	}
}

func (s EmployeeDTOCompactSerializerV2) Write(writer serialization.CompactWriter, value interface{}) {
	c, ok := value.(EmployeeDTO)
	if !ok {
		panic("not an EmployeeDTO")
	}
	writer.WriteInt32("age", c.age)
	writer.WriteInt64("id", c.id)
	surname := "sir"
	writer.WriteString("surname", &surname)
}

type EmployeeDTOCompactSerializerV3 struct{}

func (EmployeeDTOCompactSerializerV3) Type() reflect.Type {
	return reflect.TypeOf(EmployeeDTO{})
}

func (s EmployeeDTOCompactSerializerV3) TypeName() string {
	return "EmployeeDTO"
}

func (s EmployeeDTOCompactSerializerV3) Read(reader serialization.CompactReader) interface{} {
	// The serializer won't be used for reading
	return nil
}

func (s EmployeeDTOCompactSerializerV3) Write(writer serialization.CompactWriter, value interface{}) {
	c, ok := value.(EmployeeDTO)
	if !ok {
		panic("not an EmployeeDTO")
	}
	writer.WriteInt32("age", c.age)
}

// TODO: refactor the functions below to use generics when Go version requirement is raised to 1.18 --YT

func nullableBoolSlice(values ...bool) []*bool {
	r := make([]*bool, len(values)+1)
	for i, v := range values {
		r[i] = &v
	}
	r[len(values)-1] = nil
	return r
}

func nullableInt8Slice(values ...int8) []*int8 {
	r := make([]*int8, len(values)+1)
	for i, v := range values {
		r[i] = &v
	}
	r[len(values)-1] = nil
	return r
}

func nullableInt16Slice(values ...int16) []*int16 {
	r := make([]*int16, len(values)+1)
	for i, v := range values {
		r[i] = &v
	}
	r[len(values)-1] = nil
	return r
}

func nullableInt32Slice(values ...int32) []*int32 {
	r := make([]*int32, len(values)+1)
	for i, v := range values {
		r[i] = &v
	}
	r[len(values)-1] = nil
	return r
}

func nullableInt64Slice(values ...int64) []*int64 {
	r := make([]*int64, len(values)+1)
	for i, v := range values {
		r[i] = &v
	}
	r[len(values)-1] = nil
	return r
}

func nullableFloat32Slice(values ...float32) []*float32 {
	r := make([]*float32, len(values))
	for i, v := range values {
		r[i] = &v
	}
	return r
}

func nullableFloat64Slice(values ...float64) []*float64 {
	r := make([]*float64, len(values))
	for i, v := range values {
		r[i] = &v
	}
	return r
}
