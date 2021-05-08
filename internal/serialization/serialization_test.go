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

package serialization_test

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"

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
	c := &serialization.Config{
		BigEndian: true,
	}
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
	ID     int
	Person string
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
	config := &serialization.Config{
		BigEndian:         true,
		CustomSerializers: map[reflect.Type]serialization.Serializer{},
	}
	config.CustomSerializers[reflect.TypeOf((*artist)(nil)).Elem()] = customSerializer
	service := MustValue(iserialization.NewService(config)).(*iserialization.Service)
	data := MustValue(service.ToData(m)).(*iserialization.Data)
	ret := MustValue(service.ToObject(data))
	data2 := MustValue(service.ToData(p)).(*iserialization.Data)
	ret2 := MustValue(service.ToObject(data2))

	if !reflect.DeepEqual(m, ret) || !reflect.DeepEqual(p, ret2) {
		t.Error("custom serialization failed")
	}
}

func TestGlobalSerializer(t *testing.T) {
	obj := &customObject{10, "Furkan Şenharputlu"}
	config := &serialization.Config{
		BigEndian:        true,
		GlobalSerializer: &GlobalSerializer{},
	}
	service, _ := iserialization.NewService(config)
	data, _ := service.ToData(obj)
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(obj, ret) {
		t.Error("global serialization failed")
	}
}

type fake2 struct {
	Bool bool
	B    byte
	C    uint16
	D    float64
	S    int16
	F    float32
	I    int32
	L    int64
	Str  string

	Bools   []bool
	Bytes   []byte
	Chars   []uint16
	Doubles []float64
	Shorts  []int16
	Floats  []float32
	Ints    []int32
	Longs   []int64
	Strings []string

	BoolsNil   []bool
	BytesNil   []byte
	CharsNil   []uint16
	DoublesNil []float64
	ShortsNil  []int16
	FloatsNil  []float32
	IntsNil    []int32
	LongsNil   []int64
	StringsNil []string
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
	expected := &fake2{aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, aString,
		bools, bytes, chars, doubles, shorts, floats, ints, longs, strings,
		nil, nil, nil, nil, nil, nil, nil, nil, nil}
	service, err := iserialization.NewService(&serialization.Config{BigEndian: true})
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
	config := &serialization.Config{BigEndian: true}
	service, _ := iserialization.NewService(config)
	data, _ := service.ToData(id)
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(int64(id), ret) {
		t.Error("int type serialization failed")
	}
}

func TestInt64ArraySerializerWithIntArray(t *testing.T) {
	var ids = []int{15, 10, 20, 12, 35}
	config := &serialization.Config{BigEndian: true}
	service, _ := iserialization.NewService(config)
	data, _ := service.ToData(ids)
	ret, _ := service.ToObject(data)

	var ids64 = make([]int64, 5)
	for k := 0; k < 5; k++ {
		ids64[k] = int64(ids[k])
	}

	if !reflect.DeepEqual(ids64, ret) {
		t.Error("[]int type serialization failed")
	}
}

func TestSerializeData(t *testing.T) {
	data := iserialization.NewData([]byte{10, 20, 0, 30, 5, 7, 6})
	config := &serialization.Config{BigEndian: true}
	service, _ := iserialization.NewService(config)
	serializedData, _ := service.ToData(data)
	if !reflect.DeepEqual(data, serializedData) {
		t.Error("Data type should not be serialized")
	}
}

func TestUndefinedDataDeserialization(t *testing.T) {
	s, _ := iserialization.NewService(&serialization.Config{BigEndian: true})
	dataOutput := iserialization.NewPositionalObjectDataOutput(1, s, s.SerializationConfig.BigEndian)
	dataOutput.WriteInt32(0) // partition
	dataOutput.WriteInt32(-100)
	dataOutput.WriteString("Furkan")
	data := &iserialization.Data{dataOutput.ToBuffer()}
	_, err := s.ToObject(data)
	require.Errorf(t, err, "err should not be nil")
}

// MustValue returns value if err is nil, otherwise it panics.
func MustValue(value interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return value
}
