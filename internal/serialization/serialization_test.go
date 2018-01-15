// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package serialization

import (
	"bytes"
	"encoding/gob"
	. "github.com/hazelcast/hazelcast-go-client/config"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	"reflect"
	"testing"
)

const (
	MUSICIAN_TYPE = 1
	PAINTER_TYPE  = 2
)

func TestSerializationService_LookUpDefaultSerializer(t *testing.T) {
	var a int32 = 5
	var id int32 = NewSerializationService(NewSerializationConfig()).lookUpDefaultSerializer(a).Id()
	var expectedId int32 = -7
	if id != expectedId {
		t.Errorf("LookUpDefaultSerializer() returns ", id, " expected ", expectedId)
	}
}

func TestSerializationService_ToData(t *testing.T) {
	var expected int32 = 5
	c := NewSerializationConfig()
	service := NewSerializationService(c)
	data, _ := service.ToData(expected)
	var ret int32
	temp, _ := service.ToObject(data)
	ret = temp.(int32)
	if expected != ret {
		t.Errorf("ToData() returns ", ret, " expected ", expected)
	}
}

type CustomArtistSerializer struct {
}

func (s *CustomArtistSerializer) Id() int32 {
	return 10
}

func (s *CustomArtistSerializer) Read(input DataInput) (interface{}, error) {
	var network bytes.Buffer
	typ, _ := input.ReadInt32()
	data, _ := input.ReadData()
	network.Write(data.Buffer())
	dec := gob.NewDecoder(&network)
	var v artist
	if typ == MUSICIAN_TYPE {
		v = &musician{}
	} else if typ == PAINTER_TYPE {
		v = &painter{}
	}

	dec.Decode(v)
	return v, nil
}

func (s *CustomArtistSerializer) Write(output DataOutput, obj interface{}) error {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(obj)
	if err != nil {
		return err
	}
	payload := (&network).Bytes()
	output.WriteInt32(obj.(artist).Type())
	output.WriteData(NewData(payload))
	return nil
}

type customObject struct {
	Id     int
	Person string
}

type GlobalSerializer struct {
}

func (s *GlobalSerializer) Id() int32 {
	return 123
}

func (s *GlobalSerializer) Read(input DataInput) (interface{}, error) {
	var network bytes.Buffer
	data, _ := input.ReadData()
	network.Write(data.Buffer())
	dec := gob.NewDecoder(&network)
	v := &customObject{}
	dec.Decode(v)
	return v, nil
}

func (s *GlobalSerializer) Write(output DataOutput, obj interface{}) error {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(obj)
	if err != nil {
		return err
	}
	payload := (&network).Bytes()
	output.WriteData(NewData(payload))
	return nil
}

type artist interface {
	Type() int32
}

type musician struct {
	Name    string
	Surname string
}

func (*musician) Type() int32 {
	return MUSICIAN_TYPE
}

type painter struct {
	Name    string
	Surname string
}

func (*painter) Type() int32 {
	return PAINTER_TYPE
}

func TestCustomSerializer(t *testing.T) {
	m := &musician{"Furkan", "Şenharputlu"}
	p := &painter{"Leonardo", "da Vinci"}
	customSerializer := &CustomArtistSerializer{}
	config := NewSerializationConfig()

	config.AddCustomSerializer(reflect.TypeOf((*artist)(nil)).Elem(), customSerializer)
	service := NewSerializationService(config)
	data, _ := service.ToData(m)
	ret, _ := service.ToObject(data)
	data2, _ := service.ToData(p)
	ret2, _ := service.ToObject(data2)

	if !reflect.DeepEqual(m, ret) || !reflect.DeepEqual(p, ret2) {
		t.Errorf("custom serialization failed")
	}
}

func TestGlobalSerializer(t *testing.T) {
	obj := &customObject{10, "Furkan Şenharputlu"}
	config := NewSerializationConfig()
	config.SetGlobalSerializer(&GlobalSerializer{})
	service := NewSerializationService(config)
	data, _ := service.ToData(obj)
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(obj, ret) {
		t.Errorf("global serialization failed")
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
	var aBoolean bool = true
	var aByte byte = 113
	var aChar uint16 = 'x'
	var aDouble float64 = -897543.3678909
	var aShort int16 = -500
	var aFloat float32 = 900.5678
	var anInt int32 = 56789
	var aLong int64 = -50992225
	var aString string = "Pijamalı hasta, yağız şoföre çabucak güvendi.イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラムThe quick brown fox jumps over the lazy dog"

	var bools []bool = []bool{true, false, true}

	// byte is signed in Java but unsigned in Go!
	var bytes []byte = []byte{112, 4, 255, 4, 112, 221, 43}
	var chars []uint16 = []uint16{'a', 'b', 'c'}
	var doubles []float64 = []float64{-897543.3678909, 11.1, 22.2, 33.3}
	var shorts []int16 = []int16{-500, 2, 3}
	var floats []float32 = []float32{900.5678, 1.0, 2.1, 3.4}
	var ints []int32 = []int32{56789, 2, 3}
	var longs []int64 = []int64{-50992225, 1231232141, 2, 3}
	w1 := "Pijamalı hasta, yağız şoföre çabucak güvendi."
	w2 := "イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム"
	w3 := "The quick brown fox jumps over the lazy dog"
	var strings []string = []string{w1, w2, w3}
	expected := &fake2{aBoolean, aByte, aChar, aDouble, aShort, aFloat, anInt, aLong, aString,
		bools, bytes, chars, doubles, shorts, floats, ints, longs, strings,
		nil, nil, nil, nil, nil, nil, nil, nil, nil}
	service := NewSerializationService(NewSerializationConfig())
	data, _ := service.ToData(expected)
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(expected, ret) {
		t.Errorf("Gob Serializer failed")
	}

}

func TestInt64SerializerWithInt(t *testing.T) {
	var id int = 15
	config := NewSerializationConfig()
	service := NewSerializationService(config)
	data, _ := service.ToData(id)
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(int64(id), ret) {
		t.Errorf("int type serialization failed")
	}
}

func TestInt64ArraySerializerWithIntArray(t *testing.T) {
	var ids []int = []int{15, 10, 20, 12, 35}
	config := NewSerializationConfig()
	service := NewSerializationService(config)
	data, _ := service.ToData(ids)
	ret, _ := service.ToObject(data)

	var ids64 []int64 = make([]int64, 5)
	for k := 0; k < 5; k++ {
		ids64[k] = int64(ids[k])
	}

	if !reflect.DeepEqual(ids64, ret) {
		t.Errorf("[]int type serialization failed")
	}
}
