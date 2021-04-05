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

package serialization

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/stretchr/testify/assert"
)

func TestObjectDataOutput_EnsureAvailable(t *testing.T) {
	o := NewObjectDataOutput(2, nil, false)
	o.EnsureAvailable(5)
	buf := o.buffer
	expectedBuf := []byte{0, 0, 0, 0, 0}

	if !bytes.Equal(buf, expectedBuf) {
		t.Error("EnsureAvailable() makes ", buf, " expected ", expectedBuf)
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
		t.Error("ToBuffer() works wrong!")
	}
}

func TestObjectDataOutput_WriteData(t *testing.T) {
	o := NewObjectDataOutput(0, nil, false)

	data := &SerializationData{[]byte{123, 122, 33, 12}}
	o.WriteData(data)
	var expectedRet = []byte{4, 0, 0, 0, 123, 122, 33, 12}
	data.Buffer()[1] = 0
	data.Buffer()[2] = 0
	data.Buffer()[3] = 0
	if !reflect.DeepEqual(o.buffer, expectedRet) {
		t.Error("WriteData() works wrong!")
	}
}

func TestObjectDataOutput_WriteInt32(t *testing.T) {
	o := NewObjectDataOutput(4, nil, false)
	o.WriteInt32(1)
	o.WriteInt32(2)
	o.WriteInt32(3)

	if o.buffer[0] != 1 || o.buffer[4] != 2 || o.buffer[8] != 3 {
		t.Error("WriteInt32() writes to wrong position")
	}
}

func TestObjectDataInput_AssertAvailable(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 1, 2, 3}, 3, &Service{}, true)
	ret := o.AssertAvailable(2)
	if ret == nil {
		t.Errorf("AssertAvailable() should return error %v but it returns nil!", ret)
	}

}

func TestObjectDataInput_AssertAvailable2(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 1, 2, 3}, 3, &Service{}, true)
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
	var expectedRet = b
	var ret byte
	ret = i.ReadByte()
	if ret != expectedRet {
		t.Errorf("ReadByte() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadBool(t *testing.T) {
	o := NewObjectDataOutput(9, &Service{}, false)
	o.WriteFloat64(1.234)
	o.WriteBool(true)
	i := NewObjectDataInput(o.buffer, 8, &Service{}, false)
	var expectedRet = true
	var ret bool
	ret = i.ReadBool()
	if ret != expectedRet {
		t.Errorf("ReadBool() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadBoolWithPosition(t *testing.T) {
	o := NewObjectDataOutput(9, &Service{}, false)
	o.WriteFloat64(1.234)
	o.WriteBool(true)
	i := NewObjectDataInput(o.buffer, 7, &Service{}, false)
	var expectedRet = true
	var ret bool
	ret = i.ReadBoolWithPosition(8)
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
	ret := i.ReadUInt16()

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

	ret := i.ReadUInt16WithPosition(4)

	if ret != expectedRet {
		t.Errorf("ReadUInt16WithPosition() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadInt32(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0}, 4, nil, false)
	expectedRet := 4
	ret := o.ReadInt32()

	if ret != int32(expectedRet) {
		t.Errorf("ReadInt32() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadInt32WithPosition(t *testing.T) {
	o := NewObjectDataInput([]byte{0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0}, 4, nil, false)
	expectedRet := 2
	ret := o.ReadInt32WithPosition(8)

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
	var expectedRet = 3.432
	var ret float64
	ret = i.ReadFloat64()
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
	var expectedRet = 2.544
	var ret float64
	ret = i.ReadFloat64WithPosition(8)
	if ret != expectedRet {
		t.Errorf("ReadFloat64WithPosition() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadUTF(t *testing.T) {
	o := NewObjectDataOutput(0, nil, false)
	o.WriteString("Furkan Şenharputlu")
	o.WriteString("Jack")
	o.WriteString("Dani")
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	expectedRet := "Dani"
	i.ReadString()
	i.ReadString()
	ret := i.ReadString()
	if ret != expectedRet {
		t.Errorf("ReadString() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadUTF2(t *testing.T) {
	o := NewObjectDataOutput(0, nil, false)
	o.WriteString("Furkan Şenharputlu")
	o.WriteString("Jack")
	o.WriteString("⚐中💦2😭‍🙆😔")
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	expectedRet := "⚐中💦2😭‍🙆😔"
	i.ReadString()
	i.ReadString()
	ret := i.ReadString()
	if ret != expectedRet {
		t.Errorf("ReadString() returns %v expected %v", ret, expectedRet)
	}
}

func TestObjectDataInput_ReadObject(t *testing.T) {
	conf := NewConfig()
	service, _ := NewService(conf)
	o := NewObjectDataOutput(500, service, false)
	var a = 6.739
	var b byte = 125
	var c int32 = 13
	var d = true
	var e = "Hello こんにちは"
	var f = []int16{3, 4, 5, -50, -123, -34, 22, 0}
	var g = []int32{3, 2, 1, 7, 23, 56, 42, 51, 66, 76, 53, 123}
	var h = []int64{123, 25, 83, 8, -23, -47, 51, 0}
	var j = []float32{12.4, 25.5, 1.24, 3.44, 12.57, 0}
	var k = []float64{12.45675333444, 25.55677, 1.243232, 3.444666, 12.572424, 0}
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

	retA := i.ReadObject()
	retB := i.ReadObject()
	retC := i.ReadObject()
	retD := i.ReadObject()
	retE := i.ReadObject()
	retF := i.ReadObject()
	retG := i.ReadObject()
	retH := i.ReadObject()
	retJ := i.ReadObject()
	retK := i.ReadObject()

	if a != retA || b != retB || c != retC || d != retD ||
		e != retE || !reflect.DeepEqual(f, retF) || !reflect.DeepEqual(g, retG) ||
		!reflect.DeepEqual(h, retH) || !reflect.DeepEqual(j, retJ) || !reflect.DeepEqual(k, retK) {
		t.Error("There is a problem in WriteObject() or ReadObject()!")
	}
}

func TestObjectDataInput_ReadByteArray(t *testing.T) {
	var array = []byte{3, 4, 5, 25, 123, 34, 52, 0}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteByteArray(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	retArray := i.ReadByteArray()

	if !reflect.DeepEqual(array, retArray) {
		t.Error("There is a problem in WriteByteArray() or ReadByteArray()!")
	}
}

func TestObjectDataInput_ReadBoolArray(t *testing.T) {
	var array = []bool{true, false, true, true, false, false, false, true}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteBoolArray(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	retArray := i.ReadBoolArray()

	if !reflect.DeepEqual(array, retArray) {
		t.Error("There is a problem in WriteBoolArray() or ReadBoolArray()!")
	}
}

func TestObjectDataInput_ReadUInt16Array(t *testing.T) {
	var array = []uint16{65535, 413, 5, 51230, 1233, 3124, 22, 0}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteUInt16Array(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	retArray := i.ReadUInt16Array()

	if !reflect.DeepEqual(array, retArray) {
		t.Error("There is a problem in WriteUInt16Array() or ReadUInt16Array()!")
	}
}

func TestObjectDataInput_ReadInt16Array(t *testing.T) {
	var array = []int16{3, 4, 5, -50, -123, -34, 22, 0}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteInt16Array(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	retArray := i.ReadInt16Array()

	if !reflect.DeepEqual(array, retArray) {
		t.Error("There is a problem in WriteInt16Array() or ReadInt16Array()!")
	}
}

func TestObjectDataInput_ReadInt32Array(t *testing.T) {
	var array = []int32{321, 122, 14, 0, -123, -34, 67, 0}
	o := NewObjectDataOutput(50, nil, false)
	o.WriteInt32Array(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	retArray := i.ReadInt32Array()

	if !reflect.DeepEqual(array, retArray) {
		t.Error("There is a problem in WriteInt32Array() or ReadInt32Array()!")
	}
}

func TestObjectDataInput_ReadInt64Array(t *testing.T) {
	var array = []int64{123, 25, 83, 8, -23, -47, 51, 0}
	o := NewObjectDataOutput(50, nil, false)
	o.WriteInt64Array(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	retArray := i.ReadInt64Array()

	if !reflect.DeepEqual(array, retArray) {
		t.Error("There is a problem in WriteInt64Array() or ReadInt64Array()!")
	}
}

func TestObjectDataInput_ReadFloat32Array(t *testing.T) {
	var array = []float32{12.4, 25.5, 1.24, 3.44, 12.57, 0}
	o := NewObjectDataOutput(50, nil, false)
	o.WriteFloat32Array(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)

	retArray := i.ReadFloat32Array()
	if !reflect.DeepEqual(array, retArray) {
		t.Error("There is a problem in WriteFloat32Array() or ReadFloat32Array()!")
	}
}

func TestObjectDataInput_ReadFloat64Array(t *testing.T) {
	var array = []float64{12.45675333444, 25.55677, 1.243232, 3.444666, 12.572424, 0}
	o := NewObjectDataOutput(50, nil, false)
	o.WriteFloat64Array(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	retArray := i.ReadFloat64Array()

	if !reflect.DeepEqual(array, retArray) {
		t.Error("There is a problem in WriteFloat64Array() or ReadFloat64Array()!")
	}
}

func TestObjectDataInput_ReadUTFArray(t *testing.T) {
	var array = []string{"aAüÜiİıIöÖşŞçÇ", "akdha", "üğpoıuişlk", "üğpreÜaişfçxaaöc"}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteUTFArray(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	retArray := i.ReadUTFArray()

	if !reflect.DeepEqual(array, retArray) {
		t.Error("There is a problem in WriteUTFArray() or ReadUTFArray()!")
	}
}

func TestObjectDataInput_ReadData(t *testing.T) {
	o := NewObjectDataOutput(0, nil, false)
	expectedRet := &SerializationData{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}}
	o.WriteString("Dummy")
	o.WriteString("Dummy2")
	o.WriteData(expectedRet)

	i := NewObjectDataInput(o.buffer, 0, nil, false)
	i.ReadString()
	i.ReadString()
	ret := i.ReadData()
	if !reflect.DeepEqual(expectedRet, ret) {
		t.Error("There is a problem in WriteData() or ReadData()!")
	}
}

func TestPositionalObjectDataOutput_PWriteByte(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = byte(32)
	o.PWriteByte(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadByteWithPosition(15)
	assert.NoError(t, i.err)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteInt16(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = int16(32)
	o.PWriteInt16(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadInt16WithPosition(15)
	assert.NoError(t, i.err)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteInt32(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = int32(32)
	o.PWriteInt32(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadInt32WithPosition(15)
	assert.NoError(t, i.err)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteInt64(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = int64(32)
	o.PWriteInt64(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadInt64WithPosition(15)
	assert.NoError(t, i.err)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteFloat64(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = float64(32)
	o.PWriteFloat64(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadFloat64WithPosition(15)
	assert.NoError(t, i.err)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteFloat32(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = float32(32)
	o.PWriteFloat32(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadFloat32WithPosition(15)
	assert.NoError(t, i.err)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteUint16(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = uint16(32)
	o.PWriteUInt16(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadUInt16WithPosition(15)
	assert.NoError(t, i.err)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteBool(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	o.PWriteBool(15, true)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadBoolWithPosition(15)
	assert.NoError(t, i.err)
	assert.Equal(t, res, true)
}

func TestObjectDataInput_ReadingAfterError(t *testing.T) {
	o := NewObjectDataOutput(9, nil, false)
	var a byte = 120
	var b byte = 176
	o.WriteByte(a)
	o.WriteByte(b)

	i := NewObjectDataInput(o.buffer, 0, nil, false)
	i.ReadBoolArray()
	assert.Error(t, i.Error())
	i.ReadByte()
	assert.Error(t, i.Error())
	i.ReadBoolArray()
	assert.Error(t, i.Error())
	i.ReadBool()
	assert.Error(t, i.Error())
	i.ReadByteArray()
	assert.Error(t, i.Error())
	i.ReadString()
	assert.Error(t, i.Error())
	i.ReadInt32()
	assert.Error(t, i.Error())
	i.ReadObject()
	assert.Error(t, i.Error())
	i.ReadData()
	assert.Error(t, i.Error())
	i.ReadInt64()
	assert.Error(t, i.Error())
	i.ReadFloat64()
	assert.Error(t, i.Error())
	i.ReadFloat32()
	assert.Error(t, i.Error())
	i.ReadFloat32Array()
	assert.Error(t, i.Error())
	i.ReadUTFArray()
	assert.Error(t, i.Error())
	i.ReadUInt16Array()
	assert.Error(t, i.Error())
	i.ReadFloat64Array()
	assert.Error(t, i.Error())
	i.ReadInt32Array()
	assert.Error(t, i.Error())
	i.ReadInt16Array()
	assert.Error(t, i.Error())
	i.ReadInt16()
	assert.Error(t, i.Error())
	i.ReadInt64Array()
	assert.Error(t, i.Error())

	i.ReadBoolArrayWithPosition(0)
	assert.Error(t, i.Error())
	i.ReadByteWithPosition(0)
	assert.Error(t, i.Error())
	i.ReadBoolArrayWithPosition(0)
	assert.Error(t, i.Error())
	i.ReadBoolWithPosition(0)
	assert.Error(t, i.Error())
	i.ReadByteArrayWithPosition(0)
	assert.Error(t, i.Error())
	i.ReadUTFWithPosition(0)
	assert.Error(t, i.Error())
	i.ReadInt32WithPosition(0)
	assert.Error(t, i.Error())
	i.ReadInt64WithPosition(0)
	assert.Error(t, i.Error())
	i.ReadFloat64WithPosition(0)
	assert.Error(t, i.Error())
	i.ReadFloat32WithPosition(0)
	assert.Error(t, i.Error())
	i.ReadFloat32ArrayWithPosition(0)
	assert.Error(t, i.Error())
	i.ReadUTFArrayWithPosition(0)
	assert.Error(t, i.Error())
	i.ReadUInt16ArrayWithPosition(0)
	assert.Error(t, i.Error())
	i.ReadFloat64ArrayWithPosition(0)
	assert.Error(t, i.Error())
	i.ReadInt32ArrayWithPosition(0)
	assert.Error(t, i.Error())
	i.ReadInt16ArrayWithPosition(0)
	assert.Error(t, i.Error())
	i.ReadInt16WithPosition(0)
	assert.Error(t, i.Error())
	i.ReadInt64ArrayWithPosition(0)
	assert.Error(t, i.Error())

}
