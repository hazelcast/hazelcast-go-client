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

package serialization

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestObjectDataOutput_EnsureAvailable(t *testing.T) {
	o := NewObjectDataOutput(2, nil, false)
	o.EnsureAvailable(5)
	buf := o.buffer
	if len(buf) < 5 {
		t.Fatalf("expected len(buf) >= 5, but it is: %d", len(buf))
	}
}

func TestObjectDataOutput_ToBuffer(t *testing.T) {
	o := NewObjectDataOutput(2, nil, false)
	o.WriteInt32(1)
	o.WriteInt32(2)
	o.WriteInt32(3)
	o.WriteByte(5)
	o.WriteByte(6)
	if !reflect.DeepEqual(o.buffer[:o.position], o.ToBuffer()) {
		t.Error("ToBuffer() works wrong!")
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
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("AssertAvailable() should panic but it did not.")
		}
	}()
	o := NewObjectDataInput([]byte{0, 1, 2, 3}, 3, &Service{}, true)
	o.AssertAvailable(2)
}

func TestObjectDataInput_AssertAvailable2(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("AssertAvailable() should panic but it did not.")
		}
	}()
	o := NewObjectDataInput([]byte{0, 1, 2, 3}, 3, &Service{}, true)
	o.AssertAvailable(2)
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
	ret = i.ReadBoolAtPosition(8)
	if ret != expectedRet {
		t.Errorf("ReadBoolAtPosition() returns %v expected %v", ret, expectedRet)
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

	ret := i.ReadUInt16AtPosition(4)

	if ret != expectedRet {
		t.Errorf("ReadUInt16AtPosition() returns %v expected %v", ret, expectedRet)
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
	ret := o.ReadInt32AtPosition(8)

	if ret != int32(expectedRet) {
		t.Errorf("ReadInt32AtPosition() returns %v expected %v", ret, expectedRet)
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
	ret = i.ReadFloat64AtPosition(8)
	if ret != expectedRet {
		t.Errorf("ReadFloat64AtPosition() returns %v expected %v", ret, expectedRet)
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
	conf := &serialization.Config{}
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

func TestObjectDataInput_ReadStringArray(t *testing.T) {
	var array = []string{"aAüÜiİıIöÖşŞçÇ", "akdha", "üğpoıuişlk", "üğpreÜaişfçxaaöc"}
	o := NewObjectDataOutput(0, nil, false)
	o.WriteStringArray(array)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	retArray := i.ReadStringArray()

	if !reflect.DeepEqual(array, retArray) {
		t.Error("There is a problem in WriteStringArray() or ReadStringArray()!")
	}
}

func TestPositionalObjectDataOutput_PWriteByte(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = byte(32)
	o.PWriteByte(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadByteAtPosition(15)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteInt16(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = int16(32)
	o.PWriteInt16(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadInt16AtPosition(15)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteInt32(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = int32(32)
	o.PWriteInt32(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadInt32AtPosition(15)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteInt64(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = int64(32)
	o.PWriteInt64(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadInt64AtPosition(15)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteFloat64(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = float64(32)
	o.PWriteFloat64(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadFloat64AtPosition(15)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteFloat32(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = float32(32)
	o.PWriteFloat32(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadFloat32AtPosition(15)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteUint16(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	var expected = uint16(32)
	o.PWriteUInt16(15, expected)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadUInt16AtPosition(15)
	assert.Equal(t, res, expected)
}

func TestPositionalObjectDataOutput_PWriteBool(t *testing.T) {
	o := NewPositionalObjectDataOutput(100, nil, false)
	o.PWriteBool(15, true)
	i := NewObjectDataInput(o.buffer, 0, nil, false)
	res := i.ReadBoolAtPosition(15)
	assert.Equal(t, res, true)
}
