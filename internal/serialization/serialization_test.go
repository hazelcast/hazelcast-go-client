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
	"bytes"
	"encoding/gob"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hazelcast/hazelcast-go-client/internal/it"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	musicianType = 1
	painterType  = 2
)

func TestSerializationService_LookUpDefaultSerializer(t *testing.T) {
	var a int32 = 5
	service, err := iserialization.NewService(&serialization.Config{})
	if err != nil {
		t.Fatal(err)
	}
	id := service.LookUpDefaultSerializer(a).ID()
	var expectedID int32 = -7
	if id != expectedID {
		t.Error("LookUpDefaultSerializer() returns ", id, " expected ", expectedID)
	}
}

func TestSerializationService_ToData(t *testing.T) {
	var expected int32 = 5
	c := &serialization.Config{}
	service, err := iserialization.NewService(c)
	if err != nil {
		t.Fatal(err)
	}
	data, err := service.ToData(expected)
	if err != nil {
		t.Fatal(err)
	}
	temp, err := service.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	ret := temp.(int32)
	if expected != ret {
		t.Error("ToData() returns ", ret, " expected ", expected)
	}
}

type CustomArtistSerializer struct {
}

func (*CustomArtistSerializer) ID() int32 {
	return 10
}

func (s *CustomArtistSerializer) Read(input serialization.DataInput) interface{} {
	var network bytes.Buffer
	typ := input.ReadInt32()
	data := input.ReadByteArray()
	network.Write(data)
	dec := gob.NewDecoder(&network)
	var v artist
	if typ == musicianType {
		v = &musician{}
	} else if typ == painterType {
		v = &painter{}
	}
	if err := dec.Decode(v); err != nil {
		panic(err)
	}
	return v
}

func (s *CustomArtistSerializer) Write(output serialization.DataOutput, obj interface{}) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	if err := enc.Encode(obj); err != nil {
		panic(err)
	}
	payload := (&network).Bytes()
	output.WriteInt32(obj.(artist).Type())
	output.WriteByteArray(payload)
}

type customObject struct {
	Person string
	ID     int
}

type GlobalSerializer struct {
}

func (s *GlobalSerializer) ID() int32 {
	return 123
}

func (s *GlobalSerializer) Read(input serialization.DataInput) interface{} {
	var network bytes.Buffer
	data := input.ReadByteArray()
	network.Write(data)
	dec := gob.NewDecoder(&network)
	v := &customObject{}
	if err := dec.Decode(v); err != nil {
		panic(err)
	}
	return v
}

func (s *GlobalSerializer) Write(output serialization.DataOutput, obj interface{}) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	if err := enc.Encode(obj); err != nil {
		panic(err)
	}
	payload := (&network).Bytes()
	output.WriteByteArray(payload)
}

type artist interface {
	Type() int32
}

type musician struct {
	Name    string
	Surname string
}

func (*musician) Type() int32 {
	return musicianType
}

type painter struct {
	Name    string
	Surname string
}

func (*painter) Type() int32 {
	return painterType
}

func TestCustomSerializer(t *testing.T) {
	m := &musician{"Furkan", "Şenharputlu"}
	p := &painter{"Leonardo", "da Vinci"}
	customSerializer := &CustomArtistSerializer{}
	config := &serialization.Config{}
	config.SetCustomSerializer(reflect.TypeOf((*artist)(nil)).Elem(), customSerializer)
	service := it.MustValue(iserialization.NewService(config)).(*iserialization.Service)
	data := it.MustValue(service.ToData(m)).(iserialization.Data)
	ret := it.MustValue(service.ToObject(data))
	data2 := it.MustValue(service.ToData(p)).(iserialization.Data)
	ret2 := it.MustValue(service.ToObject(data2))

	if !reflect.DeepEqual(m, ret) || !reflect.DeepEqual(p, ret2) {
		t.Error("custom serialization failed")
	}
}

func TestGlobalSerializer(t *testing.T) {
	obj := &customObject{ID: 10, Person: "Furkan Şenharputlu"}
	config := &serialization.Config{}
	config.SetGlobalSerializer(&GlobalSerializer{})
	service, _ := iserialization.NewService(config)
	data, _ := service.ToData(obj)
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(obj, ret) {
		t.Error("global serialization failed")
	}
}

type fake2 struct {
	Str        string
	BoolsNil   []bool
	IntsNil    []int32
	FloatsNil  []float32
	ShortsNil  []int16
	DoublesNil []float64
	CharsNil   []uint16
	BytesNil   []byte
	LongsNil   []int64
	Bools      []bool
	Bytes      []byte
	Chars      []uint16
	Doubles    []float64
	Shorts     []int16
	Floats     []float32
	Ints       []int32
	Longs      []int64
	Strings    []string
	StringsNil []string
	L          int64
	D          float64
	I          int32
	F          float32
	S          int16
	C          uint16
	B          byte
	Bool       bool
}

func TestGobSerializer(t *testing.T) {
	var aBoolean = true
	var aByte byte = 113
	var aChar uint16 = 'x'
	var aDouble = -897543.3678909
	var aShort int16 = -500
	var aFloat float32 = 900.5678
	var anInt int32 = 56789
	var aLong int64 = -50992225
	var aString = "Pijamalı hasta, yağız şoföre çabucak güvendi.イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム" +
		"The quick brown fox jumps over the lazy dog"

	var bools = []bool{true, false, true}

	// byte is signed in Java but unsigned in Go!
	var bytes = []byte{112, 4, 255, 4, 112, 221, 43}
	var chars = []uint16{'a', 'b', 'c'}
	var doubles = []float64{-897543.3678909, 11.1, 22.2, 33.3}
	var shorts = []int16{-500, 2, 3}
	var floats = []float32{900.5678, 1.0, 2.1, 3.4}
	var ints = []int32{56789, 2, 3}
	var longs = []int64{-50992225, 1231232141, 2, 3}
	w1 := "Pijamalı hasta, yağız şoföre çabucak güvendi."
	w2 := "イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム"
	w3 := "The quick brown fox jumps over the lazy dog"
	var strings = []string{w1, w2, w3}
	expected := &fake2{Bool: aBoolean, B: aByte, C: aChar, D: aDouble, S: aShort, F: aFloat, I: anInt, L: aLong, Str: aString,
		Bools: bools, Bytes: bytes, Chars: chars, Doubles: doubles, Shorts: shorts, Floats: floats, Ints: ints, Longs: longs, Strings: strings}
	service, err := iserialization.NewService(&serialization.Config{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := service.ToData(expected)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := service.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, ret) {
		t.Error("Gob Serializer failed")
	}

}

func TestInt64SerializerWithInt(t *testing.T) {
	var id = 15
	config := &serialization.Config{}
	service := mustSerializationService(iserialization.NewService(config))
	data := mustData(service.ToData(id))
	ret := it.MustValue(service.ToObject(data))
	assert.Equal(t, int64(id), ret)
}

func TestInt64ArraySerializerWithIntArray(t *testing.T) {
	var ids = []int{15, 10, 20, 12, 35}
	config := &serialization.Config{}
	service := mustSerializationService(iserialization.NewService(config))
	data := mustData(service.ToData(ids))
	ret := it.MustValue(service.ToObject(data))
	var ids64 = make([]int64, 5)
	for k := 0; k < 5; k++ {
		ids64[k] = int64(ids[k])
	}

	if !reflect.DeepEqual(ids64, ret) {
		t.Error("[]int type serialization failed")
	}
}

func TestDefaultSerializerWithUInt(t *testing.T) {
	// XXX: This test succeeds even though uint is not a builtin serializer,
	// since the value is serialized with the default serialzer.
	// This is wrong!
	var id = uint(15)
	config := &serialization.Config{}
	service := mustSerializationService(iserialization.NewService(config))
	data := mustData(service.ToData(id))
	ret := it.MustValue(service.ToObject(data))
	assert.Equal(t, id, ret)
}

func TestIntSerializer(t *testing.T) {
	var id = 15
	config := &serialization.Config{}
	service := mustSerializationService(iserialization.NewService(config))
	data := mustData(service.ToData(id))
	ret := it.MustValue(service.ToObject(data))
	assert.Equal(t, int64(id), ret)
}

func TestSerializeData(t *testing.T) {
	data := iserialization.Data([]byte{10, 20, 0, 30, 5, 7, 6})
	config := &serialization.Config{}
	service := mustSerializationService(iserialization.NewService(config))
	serializedData, err := service.ToData(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(data, serializedData) {
		t.Error("Data type should not be serialized")
	}
}

func TestSerializeRune(t *testing.T) {
	config := &serialization.Config{}
	ss := mustSerializationService(iserialization.NewService(config))
	var target rune = 0x2318
	data, err := ss.ToData(target)
	if err != nil {
		t.Fatal(err)
	}
	value, err := ss.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, target, value)
}

func TestUndefinedDataDeserialization(t *testing.T) {
	s, _ := iserialization.NewService(&serialization.Config{})
	dataOutput := iserialization.NewPositionalObjectDataOutput(1, s, !s.SerializationConfig.LittleEndian)
	dataOutput.WriteInt32(0) // partition
	dataOutput.WriteInt32(-100)
	dataOutput.WriteString("Furkan")
	data := iserialization.Data(dataOutput.ToBuffer())
	_, err := s.ToObject(data)
	require.Errorf(t, err, "err should not be nil")
}

func mustData(value interface{}, err error) iserialization.Data {
	if err != nil {
		panic(err)
	}
	return value.(iserialization.Data)
}

func TestWithExplicitSerializer(t *testing.T) {
	compactConfig := serialization.CompactConfig{}
	serializer := EmployeeDTOCompactSerializer{}
	compactConfig.SetSerializers(serializer)
	c := &serialization.Config{
		Compact: compactConfig,
	}
	service := mustSerializationService(iserialization.NewService(c))
	obj := EmployeeDTO{age: 30, id: 102310312}
	data, err := service.ToData(obj)
	// Ensure that data is serialized as compact
	assert.EqualValues(t, data.Type(), iserialization.TypeCompact)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := service.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(obj, ret) {
		t.Error("compact serialization failed")
	}
}

func TestAllTypesWithCustomSerializer(t *testing.T) {
	compactConfig := serialization.CompactConfig{}
	compactConfig.SetSerializers(MainDTOSerializer{}, InnerDTOSerializer{}, NamedDTOSerializer{})
	c := &serialization.Config{
		Compact: compactConfig,
	}
	service := mustSerializationService(iserialization.NewService(c))
	mainDTO := NewMainDTO()
	data, err := service.ToData(mainDTO)
	if err != nil {
		t.Fatal(err)
	}
	// Ensure that data is serialized as compact
	assert.EqualValues(t, data.Type(), iserialization.TypeCompact)
	ret, err := service.ToObject(data)
	returnedMainDTO := ret.(MainDTO)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, mainDTO.b, returnedMainDTO.b)
	assert.Equal(t, mainDTO.boolean, returnedMainDTO.boolean)
	assert.Equal(t, mainDTO.s, returnedMainDTO.s)
	assert.Equal(t, mainDTO.i, returnedMainDTO.i)
	assert.Equal(t, mainDTO.l, returnedMainDTO.l)
	assert.Equal(t, mainDTO.f, returnedMainDTO.f)
	assert.Equal(t, mainDTO.d, returnedMainDTO.d)
	assert.Equal(t, mainDTO.str, returnedMainDTO.str)
	assert.Equal(t, mainDTO.p.bools, returnedMainDTO.p.bools)
	assert.Equal(t, mainDTO.p.bytes, returnedMainDTO.p.bytes)
	assert.Equal(t, mainDTO.p.shorts, returnedMainDTO.p.shorts)
	assert.Equal(t, mainDTO.p.ints, returnedMainDTO.p.ints)
	assert.Equal(t, mainDTO.p.longs, returnedMainDTO.p.longs)
	assert.Equal(t, mainDTO.p.floats, returnedMainDTO.p.floats)
	assert.Equal(t, mainDTO.p.doubles, returnedMainDTO.p.doubles)
	assert.Equal(t, mainDTO.p.strings, returnedMainDTO.p.strings)
	assert.Equal(t, mainDTO.p.nn, returnedMainDTO.p.nn)
	assert.Equal(t, mainDTO.p.bigDecimals, returnedMainDTO.p.bigDecimals)
	assert.Equal(t, mainDTO.p.localTimes, returnedMainDTO.p.localTimes)
	assert.Equal(t, mainDTO.p.localDates, returnedMainDTO.p.localDates)
	assert.Equal(t, mainDTO.p.localDateTimes, returnedMainDTO.p.localDateTimes)
	if len(mainDTO.p.offsetDateTimes) != len(returnedMainDTO.p.offsetDateTimes) {
		t.Error("innerDTO.offsetDateTimes is not equal")
	}

	for i := 0; i < len(mainDTO.p.offsetDateTimes); i++ {
		if mainDTO.p.offsetDateTimes[i] == nil && returnedMainDTO.p.offsetDateTimes[i] == nil {
			continue
		}
		odt1 := time.Time(*mainDTO.p.offsetDateTimes[i])
		odt2 := time.Time(*returnedMainDTO.p.offsetDateTimes[i])

		if !odt1.Equal(odt2) {
			t.Errorf("One of offsetDateTimes in innerDTO.offsetDateTimes is not equal, %s != %s", odt1, odt2)
		}
	}
	assert.Equal(t, mainDTO.p.nullableBools, returnedMainDTO.p.nullableBools)
	assert.Equal(t, mainDTO.p.nullableBytes, returnedMainDTO.p.nullableBytes)
	assert.Equal(t, mainDTO.p.nullableShorts, returnedMainDTO.p.nullableShorts)
	assert.Equal(t, mainDTO.p.nullableIntegers, returnedMainDTO.p.nullableIntegers)
	assert.Equal(t, mainDTO.p.nullableLongs, returnedMainDTO.p.nullableLongs)
	assert.Equal(t, mainDTO.p.nullableFloats, returnedMainDTO.p.nullableFloats)
	assert.Equal(t, mainDTO.p.nullableDoubles, returnedMainDTO.p.nullableDoubles)
	assert.Equal(t, mainDTO.bigDecimal, returnedMainDTO.bigDecimal)
	assert.Equal(t, mainDTO.localTime, returnedMainDTO.localTime)
	assert.Equal(t, mainDTO.localDate, returnedMainDTO.localDate)
	assert.Equal(t, mainDTO.localDateTime, returnedMainDTO.localDateTime)
	offsetDateTimesEqual := false
	if mainDTO.offsetDateTime == nil && returnedMainDTO.offsetDateTime == nil {
		offsetDateTimesEqual = true
	} else {
		offsetDateTimesEqual = time.Time(*mainDTO.offsetDateTime).Equal(time.Time(*returnedMainDTO.offsetDateTime))
	}
	assert.True(t, offsetDateTimesEqual, "MainDTO.offsetDateTimes are not equal")
	assert.Equal(t, mainDTO.nullableB, returnedMainDTO.nullableB)
	assert.Equal(t, mainDTO.nullableBool, returnedMainDTO.nullableBool)
	assert.Equal(t, mainDTO.nullableS, returnedMainDTO.nullableS)
	assert.Equal(t, mainDTO.nullableI, returnedMainDTO.nullableI)
	assert.Equal(t, mainDTO.nullableL, returnedMainDTO.nullableL)
	assert.Equal(t, mainDTO.nullableF, returnedMainDTO.nullableF)
	assert.Equal(t, mainDTO.nullableD, returnedMainDTO.nullableD)
}

func TestReaderReturnsDefaultValues_whenDataIsMissing(t *testing.T) {
	compactConfig := serialization.CompactConfig{}
	compactConfig.SetSerializers(NoWriteMainDTOSerializer{}, NoWriteInnerDTOSerializer{}, NamedDTOSerializer{})
	c := &serialization.Config{
		Compact: compactConfig,
	}
	service := mustSerializationService(iserialization.NewService(c))
	mainDTO := NewMainDTO()
	data, err := service.ToData(mainDTO)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := service.ToObject(data)
	returnedMainDTO := ret.(MainDTO)
	if err != nil {
		t.Fatal(err)
	}
	assert.EqualValues(t, 1, returnedMainDTO.b)
	assert.False(t, returnedMainDTO.boolean)
	assert.EqualValues(t, 1, returnedMainDTO.s)
	assert.EqualValues(t, 1, returnedMainDTO.i)
	assert.EqualValues(t, 1, returnedMainDTO.l)
	assert.EqualValues(t, 1, returnedMainDTO.f)
	assert.EqualValues(t, 1, returnedMainDTO.d)
	assert.Equal(t, "NA", *returnedMainDTO.str)
	assert.Equal(t, types.NewDecimal(big.NewInt(1), 0), *returnedMainDTO.bigDecimal)
	assert.True(t, time.Date(0, 0, 0, 1, 1, 1, 0, time.Local).Equal(time.Time(*returnedMainDTO.localTime)))
	assert.True(t, time.Date(1, 1, 1, 0, 0, 0, 0, time.Local).Equal(time.Time(*returnedMainDTO.localDate)))
	assert.True(t, time.Date(1, 1, 1, 1, 1, 1, 0, time.Local).Equal(time.Time(*returnedMainDTO.localDateTime)))
	assert.True(t, time.Date(1, 1, 1, 1, 1, 1, 1, time.FixedZone("", 3600)).Equal(time.Time(*returnedMainDTO.offsetDateTime)))
	assert.EqualValues(t, 1, *returnedMainDTO.nullableB)
	assert.False(t, *returnedMainDTO.nullableBool)
	assert.EqualValues(t, 1, *returnedMainDTO.nullableS)
	assert.EqualValues(t, 1, *returnedMainDTO.nullableI)
	assert.EqualValues(t, 1, *returnedMainDTO.nullableL)
	assert.EqualValues(t, 1, *returnedMainDTO.nullableF)
	assert.EqualValues(t, 1, *returnedMainDTO.nullableD)
	// Test InnerDTO
	innerDTO := NewInnerDTO()
	data, err = service.ToData(innerDTO)
	if err != nil {
		t.Fatal(err)
	}
	ret, err = service.ToObject(data)
	returnedInnerDTO := ret.(InnerDTO)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, returnedInnerDTO.bools, []bool{})
	assert.Equal(t, returnedInnerDTO.bytes, []int8{})
	assert.Equal(t, returnedInnerDTO.shorts, []int16{})
	assert.Equal(t, returnedInnerDTO.ints, []int32{})
	assert.Equal(t, returnedInnerDTO.longs, []int64{})
	assert.Equal(t, returnedInnerDTO.floats, []float32{})
	assert.Equal(t, returnedInnerDTO.doubles, []float64{})
	assert.Equal(t, returnedInnerDTO.strings, []*string{})
	assert.Equal(t, returnedInnerDTO.nn, []*NamedDTO{})
	assert.Equal(t, returnedInnerDTO.bigDecimals, []*types.Decimal{})
	assert.Equal(t, returnedInnerDTO.localTimes, []*types.LocalTime{})
	assert.Equal(t, returnedInnerDTO.localDates, []*types.LocalDate{})
	assert.Equal(t, returnedInnerDTO.localDateTimes, []*types.LocalDateTime{})
	assert.Equal(t, returnedInnerDTO.offsetDateTimes, []*types.OffsetDateTime{})
	assert.Equal(t, returnedInnerDTO.nullableBools, []*bool{})
	assert.Equal(t, returnedInnerDTO.nullableBytes, []*int8{})
	assert.Equal(t, returnedInnerDTO.nullableShorts, []*int16{})
	assert.Equal(t, returnedInnerDTO.nullableIntegers, []*int32{})
	assert.Equal(t, returnedInnerDTO.nullableLongs, []*int64{})
	assert.Equal(t, returnedInnerDTO.nullableFloats, []*float32{})
	assert.Equal(t, returnedInnerDTO.nullableDoubles, []*float64{})
}

func TestBits(t *testing.T) {
	compactConfig := serialization.CompactConfig{}
	compactConfig.SetSerializers(BitsDTOSerializer{})
	c := &serialization.Config{
		Compact: compactConfig,
	}
	service := mustSerializationService(iserialization.NewService(c))
	service2 := mustSerializationService(iserialization.NewService(c))
	booleans := make([]bool, 8)
	booleans[0] = true
	booleans[4] = true
	bitsDTO := BitsDTO{a: true, h: true, id: 121, booleans: booleans}
	data, err := service.ToData(bitsDTO)
	if err != nil {
		t.Fatal(err)
	}
	// hash(4) + typeid(4) + schemaId(8) + (4 byte length) + (1 bytes for 8 bits) + (4 bytes for int)
	// (4 byte length of byte array) + (1 byte for booleans array of 8 bits) + (1 byte offset bytes)
	assert.Equal(t, 31, len(data.ToByteArray()))
	// To create schema in service2
	_, err = service2.ToData(BitsDTO{})
	if err != nil {
		t.Fatal(err)
	}
	ret, err := service2.ToObject(data)
	returnedBitsDTO := ret.(BitsDTO)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(returnedBitsDTO, bitsDTO) {
		t.Fatal("BitsDTOs are not equal")
	}
}

func TestWithExplicitSerializerNested(t *testing.T) {
	compactConfig := serialization.CompactConfig{}
	compactConfig.SetSerializers(EmployeeDTOCompactSerializer{}, EmployerDTOCompactSerializer{})
	c := &serialization.Config{
		Compact: compactConfig,
	}
	service := mustSerializationService(iserialization.NewService(c))
	employeeDTO := EmployeeDTO{age: 30, id: 102310312}
	ids := make([]int64, 2)
	ids[0] = 22
	ids[1] = 44
	employeeDTOs := make([]*EmployeeDTO, 5)
	for i := 0; i < len(employeeDTOs); i++ {
		employeeDTOs[i] = &EmployeeDTO{
			age: int32(20 + i),
			id:  int64(i * 100),
		}
	}
	hiringStatus := "HIRING"
	name := "nbss"
	employerDTO := EmployerDTO{
		name:           &name,
		zcode:          40,
		hiringStatus:   &hiringStatus,
		ids:            ids,
		singleEmployee: &employeeDTO,
		otherEmployees: employeeDTOs,
	}
	data, err := service.ToData(employerDTO)
	if err != nil {
		t.Fatal(err)
	}
	ret, err := service.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	returnedEmployerDTO := ret.(EmployerDTO)
	if !reflect.DeepEqual(returnedEmployerDTO, employerDTO) {
		t.Fatal("EmployerDTOs are not equal")
	}
}
func TestSchemaEvolution_fieldAdded(t *testing.T) {
	schemaService := iserialization.NewSchemaService()
	compactConfig := serialization.CompactConfig{}
	compactConfig.SetSerializers(EmployeeDTOCompactSerializerV2{})
	c := &serialization.Config{
		Compact: compactConfig,
	}
	service := mustSerializationService(iserialization.NewService(c))
	service.SetSchemaService(schemaService)
	employeeDTO := EmployeeDTO{age: 30, id: 102310312}
	data := it.MustValue(service.ToData(employeeDTO)).(iserialization.Data)
	compactConfig2 := serialization.CompactConfig{}
	compactConfig2.SetSerializers(EmployeeDTOCompactSerializer{})
	c2 := &serialization.Config{
		Compact: compactConfig2,
	}
	service2 := mustSerializationService(iserialization.NewService(c2))
	service2.SetSchemaService(schemaService)
	ret := it.MustValue(service2.ToObject(data))
	returnedEmployeeDTO := ret.(EmployeeDTO)
	assert.Equal(t, employeeDTO.age, returnedEmployeeDTO.age)
	assert.Equal(t, employeeDTO.id, returnedEmployeeDTO.id)
}
func TestSchemaEvolution_fieldRemoved(t *testing.T) {
	schemaService := iserialization.NewSchemaService()
	compactConfig := serialization.CompactConfig{}
	compactConfig.SetSerializers(EmployeeDTOCompactSerializerV3{})
	c := &serialization.Config{
		Compact: compactConfig,
	}
	service := mustSerializationService(iserialization.NewService(c))
	service.SetSchemaService(schemaService)
	employeeDTO := EmployeeDTO{age: 30, id: 102310312}
	data := it.MustValue(service.ToData(employeeDTO)).(iserialization.Data)
	compactConfig2 := serialization.CompactConfig{}
	compactConfig2.SetSerializers(EmployeeDTOCompactSerializer{})
	c2 := &serialization.Config{
		Compact: compactConfig2,
	}
	service2 := mustSerializationService(iserialization.NewService(c2))
	service2.SetSchemaService(schemaService)
	ret := it.MustValue(service2.ToObject(data))
	returnedEmployeeDTO := ret.(EmployeeDTO)
	assert.Equal(t, employeeDTO.age, returnedEmployeeDTO.age)
	assert.EqualValues(t, 0, returnedEmployeeDTO.id)
}
