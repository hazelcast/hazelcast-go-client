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
	name string
	myint int32
}

type InnerDTO struct {
	bools []bool
	bytes []int8
	shorts []int16
	ints []int32
	longs []int64
	floats []float32
	doubles []float64
	strings []*string
	nn []*NamedDTO
	bigDecimals []*types.Decimal
	localTimes []*types.LocalTime
	localDates []*types.LocalDate
	localDateTimes []*types.LocalDateTime
	offsetDateTimes []*types.OffsetDateTime
	nullableBools []*bool
	nullableBytes []*int8
	nullableShorts []*int16
	nullableIntegers []*int32
	nullableLongs []*int64
	nullableFloats []*float32
	nullableDoubles []*float64
}

type MainDTO struct {
	b int8
	boolean bool
	s int16
	i   int32
	l   int64
	f   float32
	d   float64
	str *string
	p *InnerDTO
	bigDecimal *types.Decimal
	localTime *types.LocalTime
	localDate *types.LocalDate
	localDateTime *types.LocalDateTime
	offsetDateTime *types.OffsetDateTime
	nullableB *int8
	nullableBool *bool
	nullableS *int16
	nullableI *int32
	nullableL *int64
	nullableF *float32
	nullableD *float64
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
	nn[0] = &NamedDTO{name: "name", myint: 123}
	nn[1] = &NamedDTO{name: "name", myint: 123}

	return InnerDTO{
		bools: []bool{true, false}, bytes: []int8{0, 1, 2}, shorts: []int16{3, 4, 5},
		ints: []int32{9, 8, 7, 6}, longs: []int64{0, 1, 5, 7, 9, 11}, floats: []float32{0.6543, -3.56, 45.67},
		doubles: []float64{456.456, 789.789, 321.321}, strings: []*string{&testStr, nil}, nn: nn,
		bigDecimals: []*types.Decimal{&bigDec1, &bigDec2}, localTimes: []*types.LocalTime{&nowLocalTime, nil, &nowLocalTime2},
		localDates: []*types.LocalDate{&nowLocalDate, nil, &nowLocalDate2}, localDateTimes: []*types.LocalDateTime{&nowLocalDateTime, nil},
		offsetDateTimes: []*types.OffsetDateTime{&nowOffsetDateTime}, nullableBools: []*bool{&aBool, &aBool2, nil},
		nullableBytes: []*int8{&aSignedByte, &aSignedByte2, &aSignedByte3, nil}, nullableShorts: []*int16{&aShort, &aShort2, &aShort3, nil},
		nullableIntegers: []*int32{&anInt, &anInt2, &anInt3, &anInt4, nil}, nullableLongs: []*int64{&aLong, &aLong2, &aLong3, &aLong4, &aLong5, &aLong6, nil},
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
	
	return MainDTO{ b: 113, boolean: true, s: -500, i: 56789, l: -50992225, f: 900.5678, d: -897543.3678909,
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
	return MainDTO{
		i:   reader.ReadInt32("i"),
		str: reader.ReadString("str"),
	}
}

func (MainDTOSerializer) Write(writer serialization.CompactWriter, value interface{}) {
	c, ok := value.(MainDTO)
	if !ok {
		panic("not a MainDTO")
	}
	writer.WriteInt32("i", c.i)
	writer.WriteString("str", c.str)
}
