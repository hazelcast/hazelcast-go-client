// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"reflect"
	"testing"
)

func TestObjectDataOutput_EnsureAvailable(t *testing.T) {
	o := NewObjectDataOutput(2, nil, false)
	o.EnsureAvailable(5)
	buf := o.buffer
	expectedBuf := []byte{0, 0, 0, 0, 0}
	if bytes.Compare(buf, expectedBuf) != 0 {
		t.Errorf("EnsureAvailable() makes ", buf, " expected ", expectedBuf)
	}
}

func TestObjectDataOutput_ToBuffer(t *testing.T) {
	o := NewObjectDataOutput(2, nil, false)
	o.WriteInt32(1)
	o.WriteInt32(2)
	o.WriteInt32(3)
	o.WriteByte(5)
	o.WriteByte(6)
	if !reflect.DeepEqual(o.buffer, o.ToBuffer()) {
		t.Errorf("ToBuffer() works wrong!")
	}
}

func TestObjectDataOutput_WriteData(t *testing.T) {
	o := NewObjectDataOutput(0, nil, false)

	data := &Data{[]byte{123, 122, 33, 12}}
	o.WriteData(data)
	var expectedRet []byte = []byte{4, 0, 0, 0, 123, 122, 33, 12}
	data.Buffer()[1] = 0
	data.Buffer()[2] = 0
	data.Buffer()[3] = 0
	if !reflect.DeepEqual(o.buffer, expectedRet) {
		t.Errorf("WriteData() works wrong!")
	}
}

func TestObjectDataOutput_WriteInt32(t *testing.T) {
	o := NewObjectDataOutput(4, nil, false)
	o.WriteInt32(1)
	o.WriteInt32(2)
	o.WriteInt32(3)

	if o.buffer[0] != 1 || o.buffer[4] != 2 || o.buffer[8] != 3 {
		t.Errorf("WriteInt32() writes to wrong position")
	}
}

func TestObjectDataInput_AssertAvailable(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 1, 2, 3}, 3, &SerializationService{}, true)
	ret := o.AssertAvailable(2)
	if ret == nil {
		t.Errorf("AssertAvailable() should return error %v but it returns nil!", ret)
	}

}

func TestObjectDataInput_AssertAvailable2(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 1, 2, 3}, 3, &SerializationService{}, true)
	ret := o.AssertAvailable(2)
	if _, ok := ret.(*core.HazelcastEOFError); !ok {
		t.Errorf("AssertAvailable() should return error type *common.HazelcastEOFError but it returns %v", reflect.TypeOf(ret))
	}
}

func TestObjectDataInput_ReadByte(t *testing.T) {
	o := NewObjectDataOutput(9, nil, false)
	var a byte = 120
	var b byte = 176
	o.WriteByte(a)
	o.WriteByte(b)
	i := NewObjectDataInput(o.buffer, 1, nil, false)
	var expectedRet byte = b
	var ret byte
	ret, _ = i.ReadByte()
	if ret != expectedRet {
		t.Errorf("ReadByte() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadBool(t *testing.T) {
	o := NewObjectDataOutput(9, &SerializationService{}, false)
	o.WriteFloat64(1.234)
	o.WriteBool(true)
	i := NewObjectDataInput(o.buffer, 8, &SerializationService{}, false)
	var expectedRet bool = true
	var ret bool
	ret, _ = i.ReadBool()
	if ret != expectedRet {
		t.Errorf("ReadBool() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadBoolWithPosition(t *testing.T) {
	o := NewObjectDataOutput(9, &SerializationService{}, false)
	o.WriteFloat64(1.234)
	o.WriteBool(true)
	i := NewObjectDataInput(o.buffer, 7, &SerializationService{}, false)
	var expectedRet bool = true
	var ret bool
	ret, _ = i.ReadBoolWithPosition(8)
	if ret != expectedRet {
		t.Errorf("ReadBoolWithPosition() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadUInt16(t *testing.T) {
	o := NewObjectDataOutput(0, nil, false)
	var expectedRet uint16 = 'a'
	o.WriteInt32(5)
	o.WriteUInt16(expectedRet)
	i := NewObjectDataInput(o.buffer, 0, nil, false)

	i.ReadInt32()
	ret, _ := i.ReadUInt16()

	if ret != expectedRet {
		t.Errorf("ReadUInt16() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadUInt16WithPosition(t *testing.T) {
	o := NewObjectDataOutput(0, nil, false)
	var expectedRet uint16 = 'a'
	o.WriteInt32(5)
	o.WriteUInt16(expectedRet)
	i := NewObjectDataInput(o.buffer, 0, nil, false)

	ret, _ := i.ReadUInt16WithPosition(4)

	if ret != expectedRet {
		t.Errorf("ReadUInt16WithPosition() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadInt32(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0}, 4, nil, false)
	expectedRet := 4
	ret, _ := o.ReadInt32()

	if ret != int32(expectedRet) {
		t.Errorf("ReadInt32() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadInt32WithPosition(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0}, 4, nil, false)
	expectedRet := 2
	ret, _ := o.ReadInt32WithPosition(8)

	if ret != int32(expectedRet) {
		t.Errorf("ReadInt32WithPosition() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadFloat64(t *testing.T) {
	o := NewObjectDataOutput(24, nil, false)
	o.WriteFloat64(1.234)
	o.WriteFloat64(2.544)
	o.WriteFloat64(3.432)
	i := NewObjectDataInput(o.buffer, 16, nil, false)
	var expectedRet float64 = 3.432
	var ret float64
	ret, _ = i.ReadFloat64()
	if ret != expectedRet {
		t.Errorf("ReadFloat64() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadFloat64WithPosition(t *testing.T) {
	o := NewObjectDataOutput(24, nil, false)
	o.WriteFloat64(1.234)
	o.WriteFloat64(2.544)
	o.WriteFloat64(3.432)
	i := NewObjectDataInput(o.buffer, 16, nil, false)
	var expectedRet float64 = 2.544
	var ret float64
	ret, _ = i.ReadFloat64WithPosition(8)
	if ret != expectedRet {
		t.Errorf("ReadFloat64WithPosition() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadUTF(t *testing.T) {
	o := NewObjectDataOutput(0, nil, false)
	o.WriteUTF("Furkan Şenharputlu")
	o.WriteUTF("Jack")
	o.WriteUTF("Dani")
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	expectedRet := "Dani"
	i.ReadUTF()
	i.ReadUTF()
	ret, _ := i.ReadUTF()
	if ret != expectedRet {
		t.Errorf("ReadUTF() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadUTF2(t *testing.T) {
	o := NewObjectDataOutput(0, nil, false)
	o.WriteUTF("Furkan Şenharputlu")
	o.WriteUTF("Jack")
	o.WriteUTF("⚐中💦2😭‍🙆😔")
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	expectedRet := "⚐中💦2😭‍🙆😔"
	i.ReadUTF()
	i.ReadUTF()
	ret, _ := i.ReadUTF()
	if ret != expectedRet {
		t.Errorf("ReadUTF() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadObject(t *testing.T) {
	conf := config.NewSerializationConfig()
	service := NewSerializationService(conf)
	o := NewObjectDataOutput(500, service, false)
	var a float64 = 6.739
	var b byte = 125
	var c int32 = 13
	var d bool = true
	var e string = "Hello こんにちは"
	var f []int16 = []int16{3, 4, 5, -50, -123, -34, 22, 0}
	var g []int32 = []int32{3, 2, 1, 7, 23, 56, 42, 51, 66, 76, 53, 123}
	var h []int64 = []int64{123, 25, 83, 8, -23, -47, 51, 0}
	var j []float32 = []float32{12.4, 25.5, 1.24, 3.44, 12.57, 0}
	var k []float64 = []float64{12.45675333444, 25.55677, 1.243232, 3.444666, 12.572424, 0}
	o.WriteObject(a)
	o.WriteObject(b)
	o.WriteObject(c)
	o.WriteObject(d)
	o.WriteObject(e)
	o.WriteObject(f)
	o.WriteObject(g)
	o.WriteObject(h)
	o.WriteObject(j)
	o.WriteObject(k)
	i := NewObjectDataInput(o.buffer, 0, service, false)

	ret_a, _ := i.ReadObject()
	ret_b, _ := i.ReadObject()
	ret_c, _ := i.ReadObject()
	ret_d, _ := i.ReadObject()
	ret_e, _ := i.ReadObject()
	ret_f, _ := i.ReadObject()
	ret_g, _ := i.ReadObject()
	ret_h, _ := i.ReadObject()
	ret_j, _ := i.ReadObject()
	ret_k, _ := i.ReadObject()

	if a != ret_a || b != ret_b || c != ret_c || d != ret_d ||
		e != ret_e || !reflect.DeepEqual(f, ret_f) || !reflect.DeepEqual(g, ret_g) ||
		!reflect.DeepEqual(h, ret_h) || !reflect.DeepEqual(j, ret_j) || !reflect.DeepEqual(k, ret_k) {
		t.Errorf("There is a problem in WriteObject() or ReadObject()!")
	}

}

func TestObjectDataInput_ReadByteArray(t *testing.T) {
	var array []byte = []byte{3, 4, 5, 25, 123, 34, 52, 0}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteByteArray(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	ret_array, _ := i.ReadByteArray()

	if !reflect.DeepEqual(array, ret_array) {
		t.Errorf("There is a problem in WriteByteArray() or ReadByteArray()!")
	}
}

func TestObjectDataInput_ReadBoolArray(t *testing.T) {
	var array []bool = []bool{true, false, true, true, false, false, false, true}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteBoolArray(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	ret_array, _ := i.ReadBoolArray()
	if !reflect.DeepEqual(array, ret_array) {
		t.Errorf("There is a problem in WriteBoolArray() or ReadBoolArray()!")
	}
}

func TestObjectDataInput_ReadUInt16Array(t *testing.T) {
	var array []uint16 = []uint16{65535, 413, 5, 51230, 1233, 3124, 22, 0}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteUInt16Array(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	ret_array, _ := i.ReadUInt16Array()
	if !reflect.DeepEqual(array, ret_array) {
		t.Errorf("There is a problem in WriteUInt16Array() or ReadUInt16Array()!")
	}
}

func TestObjectDataInput_ReadInt16Array(t *testing.T) {
	var array []int16 = []int16{3, 4, 5, -50, -123, -34, 22, 0}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteInt16Array(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	ret_array, _ := i.ReadInt16Array()

	if !reflect.DeepEqual(array, ret_array) {
		t.Errorf("There is a problem in WriteInt16Array() or ReadInt16Array()!")
	}
}

func TestObjectDataInput_ReadInt32Array(t *testing.T) {
	var array []int32 = []int32{321, 122, 14, 0, -123, -34, 67, 0}
	o := NewObjectDataOutput(50, nil, false)
	o.WriteInt32Array(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	ret_array, _ := i.ReadInt32Array()
	if !reflect.DeepEqual(array, ret_array) {
		t.Errorf("There is a problem in WriteInt32Array() or ReadInt32Array()!")
	}
}

func TestObjectDataInput_ReadInt64Array(t *testing.T) {
	var array []int64 = []int64{123, 25, 83, 8, -23, -47, 51, 0}
	o := NewObjectDataOutput(50, nil, false)
	o.WriteInt64Array(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	ret_array, _ := i.ReadInt64Array()
	if !reflect.DeepEqual(array, ret_array) {
		t.Errorf("There is a problem in WriteInt64Array() or ReadInt64Array()!")
	}
}

func TestObjectDataInput_ReadFloat32Array(t *testing.T) {
	var array []float32 = []float32{12.4, 25.5, 1.24, 3.44, 12.57, 0}
	o := NewObjectDataOutput(50, nil, false)
	o.WriteFloat32Array(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	ret_array, _ := i.ReadFloat32Array()
	if !reflect.DeepEqual(array, ret_array) {
		t.Errorf("There is a problem in WriteFloat32Array() or ReadFloat32Array()!")
	}
}

func TestObjectDataInput_ReadFloat64Array(t *testing.T) {
	var array []float64 = []float64{12.45675333444, 25.55677, 1.243232, 3.444666, 12.572424, 0}
	o := NewObjectDataOutput(50, nil, false)
	o.WriteFloat64Array(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	ret_array, _ := i.ReadFloat64Array()
	if !reflect.DeepEqual(array, ret_array) {
		t.Errorf("There is a problem in WriteFloat64Array() or ReadFloat64Array()!")

	}
}

func TestObjectDataInput_ReadUTFArray(t *testing.T) {
	var array []string = []string{"aAüÜiİıIöÖşŞçÇ", "akdha", "üğpoıuişlk", "üğpreÜaişfçxaaöc"}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteUTFArray(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	ret_array, _ := i.ReadUTFArray()
	if !reflect.DeepEqual(array, ret_array) {
		t.Errorf("There is a problem in WriteUTFArray() or ReadUTFArray()!")
	}
}

func TestObjectDataInput_ReadData(t *testing.T) {
	o := NewObjectDataOutput(0, nil, false)
	expectedRet := &Data{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}}
	o.WriteUTF("Dummy")
	o.WriteUTF("Dummy2")
	o.WriteData(expectedRet)

	i := NewObjectDataInput(o.buffer, 0, nil, false)
	i.ReadUTF()
	i.ReadUTF()
	ret, _ := i.ReadData()
	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("There is a problem in WriteData() or ReadData()!")
	}

}
