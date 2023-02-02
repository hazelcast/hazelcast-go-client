/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/it"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	musicianType = 1
	painterType  = 2
)

func TestSerializationService_LookUpDefaultSerializer(t *testing.T) {
	var a int32 = 5
	service := it.MustSerializationService(iserialization.NewService(&serialization.Config{}))
	id := service.LookUpDefaultSerializer(a).ID()
	var expectedID int32 = -7
	require.Equal(t, expectedID, id)
}

func TestSerializationService_ToData(t *testing.T) {
	var v int32 = 5
	c := &serialization.Config{}
	service := it.MustSerializationService(iserialization.NewService(c))
	data := it.MustData(service.ToData(v))
	obj := it.MustValue(service.ToObject(data))
	require.Equal(t, obj, v)
}

func TestSerializationService_ToData_LittleEndianTrue(t *testing.T) {
	var v int32 = 100
	c := &serialization.Config{}
	c.LittleEndian = true
	service := it.MustSerializationService(iserialization.NewService(c))
	data := it.MustData(service.ToData(v))
	obj := it.MustValue(service.ToObject(data))
	require.Equal(t, obj, v)
}

type CustomArtistSerializer struct {
	readerCalled bool
	writerCalled bool
}

func (*CustomArtistSerializer) ID() int32 {
	return 10
}

func (s *CustomArtistSerializer) Read(input serialization.DataInput) interface{} {
	s.readerCalled = true
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
	s.writerCalled = true
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
	ser := &CustomArtistSerializer{}
	config := &serialization.Config{}
	it.Must(config.SetCustomSerializer(reflect.TypeOf((*artist)(nil)).Elem(), ser))
	service := it.MustSerializationService(iserialization.NewService(config))
	data := it.MustData(service.ToData(m))
	ret := it.MustValue(service.ToObject(data))
	require.Equal(t, m, ret)
	assert.True(t, ser.readerCalled)
	assert.True(t, ser.writerCalled)
}

func TestGlobalSerializer(t *testing.T) {
	obj := &customObject{ID: 10, Person: "Furkan Şenharputlu"}
	config := &serialization.Config{}
	config.SetGlobalSerializer(&GlobalSerializer{})
	service := it.MustSerializationService(iserialization.NewService(config))
	data := it.MustData(service.ToData(obj))
	ret := it.MustValue(service.ToObject(data))
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
	service := it.MustSerializationService(iserialization.NewService(&serialization.Config{}))
	data := it.MustData(service.ToData(expected))
	ret := it.MustValue(service.ToObject(data))
	if !reflect.DeepEqual(expected, ret) {
		t.Error("Gob Serializer failed")
	}
}

func TestInt64SerializerWithInt(t *testing.T) {
	var id = 15
	config := &serialization.Config{}
	service := it.MustSerializationService(iserialization.NewService(config))
	data := it.MustData(service.ToData(id))
	ret := it.MustValue(service.ToObject(data))
	assert.Equal(t, int64(id), ret)
}

func TestInt64ArraySerializerWithIntArray(t *testing.T) {
	var ids = []int{15, 10, 20, 12, 35}
	config := &serialization.Config{}
	service := it.MustSerializationService(iserialization.NewService(config))
	data := it.MustData(service.ToData(ids))
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
	service := it.MustSerializationService(iserialization.NewService(config))
	data := it.MustData(service.ToData(id))
	ret := it.MustValue(service.ToObject(data))
	assert.Equal(t, id, ret)
}

func TestIntSerializer(t *testing.T) {
	var id = 15
	config := &serialization.Config{}
	service := it.MustSerializationService(iserialization.NewService(config))
	data := it.MustData(service.ToData(id))
	ret := it.MustValue(service.ToObject(data))
	assert.Equal(t, int64(id), ret)
}

func TestSerializeData(t *testing.T) {
	data := iserialization.Data([]byte{10, 20, 0, 30, 5, 7, 6})
	config := &serialization.Config{}
	service := it.MustSerializationService(iserialization.NewService(config))
	serializedData := it.MustData(service.ToData(data))
	if !reflect.DeepEqual(data, serializedData) {
		t.Error("Data type should not be serialized")
	}
}

func TestSerializeRune(t *testing.T) {
	config := &serialization.Config{}
	ss := it.MustSerializationService(iserialization.NewService(config))
	var target rune = 0x2318
	data := it.MustData(ss.ToData(target))
	value := it.MustValue(ss.ToObject(data))
	assert.Equal(t, target, value)
}

func TestUndefinedDataDeserialization(t *testing.T) {
	s := it.MustSerializationService(iserialization.NewService(&serialization.Config{}))
	dataOutput := iserialization.NewPositionalObjectDataOutput(1, s, !s.SerializationConfig.LittleEndian)
	dataOutput.WriteInt32BigEndian(0)    // partition
	dataOutput.WriteInt32BigEndian(-100) // serializer id
	dataOutput.WriteString("Furkan")
	data := iserialization.Data(dataOutput.ToBuffer())
	_, err := s.ToObject(data)
	require.Errorf(t, err, "err should not be nil")
}
