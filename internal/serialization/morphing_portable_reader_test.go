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
	"errors"
	"reflect"
	"testing"

	hzerrors "github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestMorphingPortableReader_ReadByte(t *testing.T) {
	expectedRet := byte(12)
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "type", serialization.TypeByte,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("type", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadByte("type")
	if expectedRet != ret {
		t.Errorf("ReadByte() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadByteWithEmptyFieldName(t *testing.T) {
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "type", serialization.TypeByte,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("type", 12)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadByte("")
	expectedRet := byte(0)
	if expectedRet != ret {
		t.Errorf("ReadByte() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadByteWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		expectedRet := true
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "type", serialization.TypeBool,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteBool("type", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
		mpr := NewMorphingPortableReader(nil, i, classDef)
		mpr.ReadByte("type")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadByte() should return error type hzerror.ErrSerialization")
	}
}

func TestMorphingPortableReader_ReadBool(t *testing.T) {
	expectedRet := true
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "isReady", serialization.TypeBool,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBool("isReady", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadBool("isReady")
	if expectedRet != ret {
		t.Errorf("ReadBool() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadBoolWithEmptyFieldName(t *testing.T) {
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "isReady", serialization.TypeBool,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBool("isReady", true)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadBool("")
	expectedRet := false
	if expectedRet != ret {
		t.Errorf("ReadBool() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadBoolWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet byte = 23
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "type", serialization.TypeByte,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteByte("type", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
		mpr := NewMorphingPortableReader(nil, i, classDef)
		mpr.ReadBool("type")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadBool() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadUInt16(t *testing.T) {
	var expectedRet uint16 = 'E'
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "char", serialization.TypeUint16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("char", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewMorphingPortableReader(nil, i, classDef)
	ret := pr.ReadUInt16("char")
	if expectedRet != ret {
		t.Errorf("ReadUInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUInt16WithEmptyFieldName(t *testing.T) {
	var value uint16 = 22
	var expectedRet uint16
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "char", serialization.TypeUint16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("char", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	pr := NewMorphingPortableReader(nil, i, classDef)
	ret := pr.ReadUInt16("")
	if expectedRet != ret {
		t.Errorf("ReadUInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUInt16WithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "char", serialization.TypeInt16,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteInt16("char", 23)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
		pr := NewMorphingPortableReader(nil, i, classDef)
		pr.ReadUInt16("char")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadUInt16() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadInt16FromByte(t *testing.T) {
	var expectedRet int16 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeByte,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("age", byte(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt16("age")
	if expectedRet != ret {
		t.Errorf("ReadInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt16FromInt16(t *testing.T) {
	var expectedRet int16 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt16("age")
	if expectedRet != ret {
		t.Errorf("ReadInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt16WithEmptyFieldName(t *testing.T) {
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", 22)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt16("")
	var expectedRet int16
	if expectedRet != ret {
		t.Errorf("ReadInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt16WithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var value int64 = 22
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt64,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteInt64("age", value)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
		mpr := NewMorphingPortableReader(nil, i, classDef)
		mpr.ReadInt16("age")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadInt16() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadInt32FromByte(t *testing.T) {
	var expectedRet int32 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeByte,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("age", byte(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt32("age")
	if expectedRet != ret {
		t.Errorf("ReadInt32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32FromUInt16(t *testing.T) {
	var expectedRet uint16 = 'a'
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "letter", serialization.TypeUint16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("letter", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt32("letter")
	if int32(expectedRet) != ret {
		t.Errorf("ReadInt32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32FromInt16(t *testing.T) {
	var expectedRet int32 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", int16(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt32("age")
	if expectedRet != ret {
		t.Errorf("ReadInt32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32FromInt32(t *testing.T) {
	var expectedRet int32 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt32,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt32("age")
	if expectedRet != ret {
		t.Errorf("ReadInt32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32WithEmptyFieldName(t *testing.T) {
	var value int32 = 22
	var expectedRet int32
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", int16(value))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt32("")
	if expectedRet != ret {
		t.Errorf("ReadInt32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32WithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var value int64 = 22
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt64,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteInt64("age", value)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

		mpr := NewMorphingPortableReader(nil, i, classDef)
		mpr.ReadInt32("age")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadInt32() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadInt64FromByte(t *testing.T) {
	var expectedRet int64 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeByte,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("age", byte(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt64("age")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64FromUInt16(t *testing.T) {
	var expectedRet int64 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeUint16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("age", uint16(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt64("age")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64FromInt16(t *testing.T) {
	var expectedRet int64 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", int16(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt64("age")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64FromInt32(t *testing.T) {
	var expectedRet int64 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt32,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32("age", int32(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt64("age")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64FromInt64(t *testing.T) {
	var expectedRet int64 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt64,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt64("age")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64WithEmptyFieldName(t *testing.T) {
	var value int64 = 22
	var expectedRet int64
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeUint16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("age", uint16(value))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt64("")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64WithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var value float32 = 22.23
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeFloat32,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteFloat32("age", value)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

		mpr := NewMorphingPortableReader(nil, i, classDef)
		mpr.ReadInt64("age")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadInt64() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadFloat32FromByte(t *testing.T) {
	var expectedRet byte = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeByte,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat32("age")
	if float32(expectedRet) != ret {
		t.Errorf("ReadFloat32() returns %f expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32FromUInt16(t *testing.T) {
	var expectedRet uint16 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeUint16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat32("age")
	if float32(expectedRet) != ret {
		t.Errorf("ReadFloat32() returns %f expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32FromInt16(t *testing.T) {
	var expectedRet int16 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat32("age")
	if float32(expectedRet) != ret {
		t.Errorf("ReadFloat32() returns %f expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32FromInt32(t *testing.T) {
	var expectedRet int32 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt32,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat32("age")
	if float32(expectedRet) != ret {
		t.Errorf("ReadFloat32() returns %f expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32FromFloat32(t *testing.T) {
	var expectedRet float32 = 22.5
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeFloat32,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat32("age")
	if expectedRet != ret {
		t.Errorf("ReadFloat32() returns %f expected %f", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32WithEmptyFieldName(t *testing.T) {
	var value float32 = 22.5
	var expectedRet float32
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeFloat32,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32("age", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat32("")
	if expectedRet != ret {
		t.Errorf("ReadFloat32() returns %f expected %f", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32WithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet = 22.5
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeFloat64,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteFloat64("age", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

		mpr := NewMorphingPortableReader(nil, i, classDef)
		mpr.ReadFloat32("age")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadFloat32() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadFloat64FromByte(t *testing.T) {
	var expectedRet byte = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "point", serialization.TypeByte,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat64("point")
	if float64(expectedRet) != ret {
		t.Errorf("ReadFloat64() returns %f expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64FromUInt16(t *testing.T) {
	var expectedRet uint16 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "point", serialization.TypeUint16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat64("point")
	if float64(expectedRet) != ret {
		t.Errorf("ReadFloat64() returns %f expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64FromInt16(t *testing.T) {
	var expectedRet int16 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "point", serialization.TypeInt16,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat64("point")
	if float64(expectedRet) != ret {
		t.Errorf("ReadFloat64() returns %f expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64FromInt32(t *testing.T) {
	var expectedRet int32 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "point", serialization.TypeInt32,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat64("point")
	if float64(expectedRet) != ret {
		t.Errorf("ReadFloat64() returns %f expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64FromInt64(t *testing.T) {
	var expectedRet int64 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "point", serialization.TypeInt64,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat64("point")
	if float64(expectedRet) != ret {
		t.Errorf("ReadFloat64() returns %f expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64FromFloat32(t *testing.T) {
	var expectedRet float32 = 22.43
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "point", serialization.TypeFloat32,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat64("point")
	if float64(expectedRet) != ret {
		t.Errorf("ReadFloat64() returns %f expected %f", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64FromFloat64(t *testing.T) {
	var expectedRet = 22.43
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "point", serialization.TypeFloat64,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat64("point")
	if expectedRet != ret {
		t.Errorf("ReadFloat64() returns %f expected %f", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64WithEmptyFieldName(t *testing.T) {
	var value = 22.43
	var expectedRet float64
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "point", serialization.TypeFloat64,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64("point", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadFloat64("")
	if expectedRet != ret {
		t.Errorf("ReadFloat64() returns %f expected %f", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64WithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		expectedRet := true
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeBool,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteBool("age", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

		mpr := NewMorphingPortableReader(nil, i, classDef)
		mpr.ReadFloat64("age")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadFloat64() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadString(t *testing.T) {
	var expectedRet = "Furkan"
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "engineer", serialization.TypeString,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteString("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadString("engineer")
	if ret != expectedRet {
		t.Errorf("ReadString() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadStringWithEmptyFieldName(t *testing.T) {
	var value = "Furkan"
	var expectedRet string
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "engineer", serialization.TypeString,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteString("engineer", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadString("")
	if ret != expectedRet {
		t.Errorf("ReadString() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadStringWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet int16 = 12
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "engineer", serialization.TypeInt16,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteInt16("engineer", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

		mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
		mpr.ReadString("engineer")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadString() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadPortable(t *testing.T) {
	var expectedRet = &student{id: 10, age: 22, name: "Furkan Şenharputlu"}
	config := &serialization.Config{PortableFactories: []serialization.PortableFactory{
		&portableFactory1{},
	}}
	classDef := serialization.NewClassDefinition(2, 1, 3)
	service, _ := NewService(config)
	classDef.AddField(NewFieldDefinition(0, "engineer", serialization.TypePortable,
		classDef.FactoryID, classDef.ClassID, classDef.Version))

	o := NewPositionalObjectDataOutput(0, service, false)
	serializer, _ := service.FindSerializerFor(expectedRet)
	pw := NewDefaultPortableWriter(serializer.(*PortableSerializer), o, classDef)
	pw.WritePortable("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, service, false)

	mpr := NewMorphingPortableReader(serializer.(*PortableSerializer), i, pw.classDefinition)
	ret := mpr.ReadPortable("engineer")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortable() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadPortableWithEmptyFieldName(t *testing.T) {
	var value serialization.Portable = &student{id: 10, age: 22, name: "Furkan Şenharputlu"}
	var expectedRet serialization.Portable
	config := &serialization.Config{PortableFactories: []serialization.PortableFactory{
		&portableFactory1{},
	}}
	classDef := serialization.NewClassDefinition(2, 1, 3)
	service, _ := NewService(config)
	classDef.AddField(NewFieldDefinition(0, "engineer", serialization.TypePortable,
		classDef.FactoryID, classDef.ClassID, classDef.Version))

	o := NewPositionalObjectDataOutput(0, service, false)
	serializer, _ := service.FindSerializerFor(value)
	pw := NewDefaultPortableWriter(serializer.(*PortableSerializer), o, classDef)
	pw.WritePortable("engineer", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, service, false)

	mpr := NewMorphingPortableReader(serializer.(*PortableSerializer), i, pw.classDefinition)
	ret := mpr.ReadPortable("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortable() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadPortableWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet int16 = 12
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "engineer", serialization.TypeInt16,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteInt16("engineer", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
		mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
		mpr.ReadPortable("engineer")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadPortable() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadByteArray(t *testing.T) {
	var expectedRet = []byte{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeByteArray,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByteArray("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadByteArray("types")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadByteArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadByteArrayWithEmptyFieldName(t *testing.T) {
	var value = []byte{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	var expectedRet []byte
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeByteArray,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByteArray("types", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadByteArray("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadByteArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadByteArrayWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet = []int32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeInt32Array,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteInt32Array("types", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
		mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
		mpr.ReadByteArray("types")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadByteArray() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadBoolArray(t *testing.T) {
	var expectedRet = []bool{true, true, false, true, false, false, false, true, false, true, true}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "areReady", serialization.TypeBoolArray,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBoolArray("areReady", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadBoolArray("areReady")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadBoolArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadBoolArrayWithEmptyFieldName(t *testing.T) {
	var value = []bool{true, true, false, true, false, false, false, true, false, true, true}
	var expectedRet []bool
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "areReady", serialization.TypeBoolArray,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBoolArray("areReady", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadBoolArray("")
	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadBoolArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadBoolArrayWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet = []int32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeInt32Array,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteInt32Array("types", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
		mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
		mpr.ReadBoolArray("types")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadBoolArray() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadUInt16Array(t *testing.T) {
	var expectedRet = []uint16{'^', '%', '#', '!', '$'}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "scores", serialization.TypeUInt16Array,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadUInt16Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadUInt16Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUInt16ArrayWithEmptyFieldName(t *testing.T) {
	var value = []uint16{'^', '%', '#', '!', '$'}
	var expectedRet []uint16
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "scores", serialization.TypeUInt16Array,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16Array("scores", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadUInt16Array("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadUInt16Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUInt16ArrayWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet = []int32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeInt32Array,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteInt32Array("types", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

		mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
		mpr.ReadUInt16Array("types")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadUInt16Array() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadInt16Array(t *testing.T) {
	var expectedRet = []int16{9432, 12, 34, 126, 7, 343, 2, 0, 1120, 222, 440}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "scores", serialization.TypeInt16Array,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadInt16Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt16Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt16ArrayWithEmptyFieldName(t *testing.T) {
	var value = []int16{9432, 12, 34, 126, 7, 343, 2, 0, 1120, 222, 440}
	var expectedRet []int16
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "scores", serialization.TypeInt16Array,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16Array("scores", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadInt16Array("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt16Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt16ArrayWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet = []int32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeInt32Array,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteInt32Array("types", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

		mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
		mpr.ReadInt16Array("types")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadInt16Array() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadInt32Array(t *testing.T) {
	var expectedRet = []int32{9432, 12, 34, 6123, 45367, 31341, 43142, 78690, 16790, 362, 0}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "scores", serialization.TypeInt32Array,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadInt32Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt32Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32ArrayWithEmptyFieldName(t *testing.T) {
	var value = []int32{9432, 12, 34, 6123, 45367, 31341, 43142, 78690, 16790, 362, 0}
	var expectedRet []int32
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "scores", serialization.TypeInt32Array,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("scores", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadInt32Array("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt32Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32ArrayWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet = []int64{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeInt64Array,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteInt64Array("types", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

		mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
		mpr.ReadInt32Array("types")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadInt32Array() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadInt64Array(t *testing.T) {
	var expectedRet = []int64{9412332, 929812, 34, 61223493, 4523367, 31235341, 46423142, 78690, 16790, 3662, 0}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "scores", serialization.TypeInt64Array,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadInt64Array("scores")
	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt64Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64ArrayWithEmptyFieldName(t *testing.T) {
	var value = []int64{9412332, 929812, 34, 61223493, 4523367, 31235341, 46423142, 78690, 16790, 3662, 0}
	var expectedRet []int64
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "scores", serialization.TypeInt64Array,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64Array("scores", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadInt64Array("")
	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt64Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64ArrayWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet = []float32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeFloat32Array,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteFloat32Array("types", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
		mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
		mpr.ReadInt64Array("types")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadInt64Array() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadFloat32Array(t *testing.T) {
	var expectedRet = []float32{12.1431, 1212.3, 34, 6123, 4.5367, 3.1341, 43.142, 786.90, 16.790, 3.62, 0}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "longitude", serialization.TypeFloat32Array,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32Array("longitude", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadFloat32Array("longitude")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadFloat32Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32ArrayWithEmptyFieldName(t *testing.T) {
	var value = []float32{12.1431, 1212.3, 34, 6123, 4.5367, 3.1341, 43.142, 786.90, 16.790, 3.62, 0}
	var expectedRet []float32
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "longitude", serialization.TypeFloat32Array,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32Array("longitude", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadFloat32Array("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadFloat32Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32ArrayWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet = []float64{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeFloat64Array,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteFloat64Array("types", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
		mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
		mpr.ReadFloat32Array("types")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadFloat32Array() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadFloat64Array(t *testing.T) {
	var expectedRet = []float64{12234.1431, 121092.3, 34, 6123, 499.5364327, 3.1323441, 43.142, 799986.90, 16.790, 3.9996342, 0}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "longitude", serialization.TypeFloat64Array,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64Array("longitude", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadFloat64Array("longitude")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadFloat64Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64ArrayWithEmptyFieldName(t *testing.T) {
	var value = []float64{12234.1431, 121092.3, 34, 6123, 499.5364327, 3.1323441, 43.142, 799986.90, 16.790, 3.9996342, 0}
	var expectedRet []float64
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "longitude", serialization.TypeFloat64Array,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64Array("longitude", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadFloat64Array("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadFloat64Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64ArrayWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet = []float32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeFloat32Array,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteFloat32Array("types", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)
		mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
		mpr.ReadFloat64Array("types")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadFloat64Array() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadStringArray(t *testing.T) {
	var expectedRet = []string{"Furkan Şenharputlu", "こんにちは", "おはようございます", "今晩は"}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "words", serialization.TypeStringArray,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteStringArray("words", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadStringArray("words")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadStringArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadStringArrayWithEmptyFieldName(t *testing.T) {
	var value = []string{"Furkan Şenharputlu", "こんにちは", "おはようございます", "今晩は"}
	var expectedRet []string
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "words", serialization.TypeStringArray,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteStringArray("words", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadStringArray("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadStringArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadStringArrayWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet = []float64{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeFloat64Array,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteFloat64Array("types", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

		mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
		mpr.ReadStringArray("types")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadStringArray() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestMorphingPortableReader_ReadPortableArray(t *testing.T) {
	var expectedRet = []serialization.Portable{
		&student{id: 10, age: 22, name: "Furkan Şenharputlu"},
		&student{id: 11, age: 20, name: "Jack Purcell"},
	}
	config := &serialization.Config{PortableFactories: []serialization.PortableFactory{
		&portableFactory1{},
	}}
	classDef := serialization.NewClassDefinition(2, 1, 3)
	service, _ := NewService(config)
	classDef.AddField(NewFieldDefinition(0, "engineers", serialization.TypePortableArray,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	serializer, err := NewPortableSerializer(service, config.PortableFactories, 0)
	if err != nil {
		t.Fatal(err)
	}
	pw := NewDefaultPortableWriter(serializer, o, classDef)
	pw.WritePortableArray("engineers", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(serializer, i, pw.classDefinition)
	ret := mpr.ReadPortableArray("engineers")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortableArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadPortableArrayWithEmptyFieldName(t *testing.T) {
	var value = []serialization.Portable{
		&student{id: 10, age: 22, name: "Furkan Şenharputlu"},
		&student{id: 11, age: 20, name: "Jack Purcell"},
	}
	var expectedRet []serialization.Portable
	config := &serialization.Config{PortableFactories: []serialization.PortableFactory{
		&portableFactory1{},
	}}
	classDef := serialization.NewClassDefinition(2, 1, 3)
	service, _ := NewService(config)
	classDef.AddField(NewFieldDefinition(0, "engineers", serialization.TypePortableArray,
		classDef.FactoryID, classDef.ClassID, classDef.Version))
	o := NewPositionalObjectDataOutput(0, nil, false)
	serializer, err := NewPortableSerializer(service, config.PortableFactories, 0)
	if err != nil {
		t.Fatal(err)
	}
	pw := NewDefaultPortableWriter(serializer, o, classDef)
	pw.WritePortableArray("engineers", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(serializer, i, pw.classDefinition)
	ret := mpr.ReadPortableArray("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortableArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadPortableArrayWithIncompatibleClassChangeError(t *testing.T) {
	err := captureErr(func() {
		var expectedRet = []float64{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
		classDef := serialization.NewClassDefinition(1, 2, 3)
		classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeFloat64Array,
			classDef.FactoryID, classDef.ClassID, classDef.Version))
		o := NewPositionalObjectDataOutput(0, nil, false)
		pw := NewDefaultPortableWriter(nil, o, classDef)
		pw.WriteFloat64Array("types", expectedRet)
		i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

		mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
		mpr.ReadPortableArray("types")
	})
	if !errors.Is(err, hzerrors.ErrSerialization) {
		t.Errorf("ReadPortableArray() should return error type hzerror.ErrSerialization but it returns: %s", reflect.TypeOf(err))
	}
}

func TestNewMorphingPortableReader(t *testing.T) {
	t.SkipNow()
	s := &student{id: 10, age: 22, name: "Furkan Şenharputlu"}
	config := &serialization.Config{PortableFactories: []serialization.PortableFactory{
		&portableFactory1{},
	}}
	service, err := NewService(config)
	if err != nil {
		t.Fatal(err)
	}
	data, err := service.ToData(s)
	if err != nil {
		t.Fatal(err)
	}
	service.SerializationConfig.PortableVersion = 1
	expectedRet := &student2{id: 10, age: 22, name: "Furkan Şenharputlu"}
	ret, err := service.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expectedRet, ret) {
		t.Error("MorphingPortableReader failed")
	}
}

func captureErr(f func()) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = makeError(v)
		}
	}()
	f()
	return nil
}
