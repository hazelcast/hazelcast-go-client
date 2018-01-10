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
	. "github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization/classdef"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	"reflect"
	"testing"
)

func TestMorphingPortableReader_ReadByte(t *testing.T) {
	var expectedRet byte = 12
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "type", BYTE, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("type", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadByte("type")
	if expectedRet != ret {
		t.Errorf("ReadByte() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadByteWithEmptyFieldName(t *testing.T) {
	var value byte = 12
	var expectedRet byte = 0
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "type", BYTE, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("type", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadByte("")
	if expectedRet != ret {
		t.Errorf("ReadByte() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadByteWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet bool = true
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "type", BOOL, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBool("type", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	_, err := mpr.ReadByte("type")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadByte() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadBool(t *testing.T) {
	var expectedRet bool = true
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "isReady", BOOL, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBool("isReady", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadBool("isReady")

	if expectedRet != ret {
		t.Errorf("ReadBool() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadBoolWithEmptyFieldName(t *testing.T) {
	var value bool = true
	var expectedRet bool = false
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "isReady", BOOL, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBool("isReady", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadBool("")

	if expectedRet != ret {
		t.Errorf("ReadBool() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadBoolWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet byte = 23
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "type", BYTE, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("type", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	_, err := mpr.ReadBool("type")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadBool() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadUInt16(t *testing.T) {
	var expectedRet uint16 = 'E'
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "char", UINT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("char", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := pr.ReadUInt16("char")
	if expectedRet != ret {
		t.Errorf("ReadUInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUInt16WithEmptyFieldName(t *testing.T) {
	var value uint16 = 22
	var expectedRet uint16 = 0
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "char", UINT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("char", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := pr.ReadUInt16("")
	if expectedRet != ret {
		t.Errorf("ReadUInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUInt16WithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet int16 = 23
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "char", INT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("char", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewMorphingPortableReader(nil, i, classDef)
	_, err := pr.ReadUInt16("char")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadUInt16() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadInt16FromByte(t *testing.T) {
	var expectedRet int16 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", BYTE, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("age", byte(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt16("age")
	if expectedRet != ret {
		t.Errorf("ReadInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt16FromInt16(t *testing.T) {
	var expectedRet int16 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", INT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt16("age")
	if expectedRet != ret {
		t.Errorf("ReadInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt16WithEmptyFieldName(t *testing.T) {
	var value int16 = 22
	var expectedRet int16 = 0
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", INT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt16("")
	if expectedRet != ret {
		t.Errorf("ReadInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt16WithIncompatibleClassChangeError(t *testing.T) {
	var value int64 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", INT64, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64("age", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	_, err := mpr.ReadInt16("age")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadInt16() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadInt32FromByte(t *testing.T) {
	var expectedRet int32 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", BYTE, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("age", byte(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt32("age")
	if expectedRet != ret {
		t.Errorf("ReadInt32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32FromUInt16(t *testing.T) {
	var expectedRet uint16 = 'a'
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "letter", UINT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("letter", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt32("letter")
	if int32(expectedRet) != ret {
		t.Errorf("ReadInt32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32FromInt16(t *testing.T) {
	var expectedRet int32 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", INT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", int16(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt32("age")
	if expectedRet != ret {
		t.Errorf("ReadInt32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32FromInt32(t *testing.T) {
	var expectedRet int32 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", INT32, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt32("age")
	if expectedRet != ret {
		t.Errorf("ReadInt32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32WithEmptyFieldName(t *testing.T) {
	var value int32 = 22
	var expectedRet int32 = 0
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", INT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", int16(value))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt32("")
	if expectedRet != ret {
		t.Errorf("ReadInt32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32WithIncompatibleClassChangeError(t *testing.T) {
	var value int64 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", INT64, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64("age", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	_, err := mpr.ReadInt32("age")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadInt32() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadInt64FromByte(t *testing.T) {
	var expectedRet int64 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", BYTE, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("age", byte(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt64("age")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64FromUInt16(t *testing.T) {
	var expectedRet int64 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", UINT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("age", uint16(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt64("age")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64FromInt16(t *testing.T) {
	var expectedRet int64 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", INT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", int16(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt64("age")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64FromInt32(t *testing.T) {
	var expectedRet int64 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", INT32, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32("age", int32(expectedRet))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt64("age")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64FromInt64(t *testing.T) {
	var expectedRet int64 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", INT64, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt64("age")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64WithEmptyFieldName(t *testing.T) {
	var value int64 = 22
	var expectedRet int64 = 0
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", UINT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("age", uint16(value))
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadInt64("")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64WithIncompatibleClassChangeError(t *testing.T) {
	var value float32 = 22.23
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", FLOAT32, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32("age", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	_, err := mpr.ReadInt64("age")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadInt64() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadFloat32FromByte(t *testing.T) {
	var expectedRet byte = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", BYTE, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat32("age")
	if float32(expectedRet) != ret {
		t.Errorf("ReadFloat32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32FromUInt16(t *testing.T) {
	var expectedRet uint16 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", UINT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat32("age")
	if float32(expectedRet) != ret {
		t.Errorf("ReadFloat32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32FromInt16(t *testing.T) {
	var expectedRet int16 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", INT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat32("age")
	if float32(expectedRet) != ret {
		t.Errorf("ReadFloat32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32FromInt32(t *testing.T) {
	var expectedRet int32 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", INT32, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat32("age")
	if float32(expectedRet) != ret {
		t.Errorf("ReadFloat32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32FromFloat32(t *testing.T) {
	var expectedRet float32 = 22.5
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", FLOAT32, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat32("age")
	if expectedRet != ret {
		t.Errorf("ReadFloat32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32WithEmptyFieldName(t *testing.T) {
	var value float32 = 22.5
	var expectedRet float32 = 0
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", FLOAT32, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32("age", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat32("")
	if expectedRet != ret {
		t.Errorf("ReadFloat32() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32WithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet float64 = 22.5
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", FLOAT64, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	_, err := mpr.ReadFloat32("age")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadFloat32() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadFloat64FromByte(t *testing.T) {
	var expectedRet byte = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "point", BYTE, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat64("point")
	if float64(expectedRet) != ret {
		t.Errorf("ReadFloat64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64FromUInt16(t *testing.T) {
	var expectedRet uint16 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "point", UINT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat64("point")
	if float64(expectedRet) != ret {
		t.Errorf("ReadFloat64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64FromInt16(t *testing.T) {
	var expectedRet int16 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "point", INT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat64("point")
	if float64(expectedRet) != ret {
		t.Errorf("ReadFloat64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64FromInt32(t *testing.T) {
	var expectedRet int32 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "point", INT32, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat64("point")
	if float64(expectedRet) != ret {
		t.Errorf("ReadFloat64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64FromInt64(t *testing.T) {
	var expectedRet int64 = 22
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "point", INT64, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat64("point")
	if float64(expectedRet) != ret {
		t.Errorf("ReadFloat64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64FromFloat32(t *testing.T) {
	var expectedRet float32 = 22.43
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "point", FLOAT32, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat64("point")
	if float64(expectedRet) != ret {
		t.Errorf("ReadFloat64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64FromFloat64(t *testing.T) {
	var expectedRet float64 = 22.43
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "point", FLOAT64, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64("point", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat64("point")
	if expectedRet != ret {
		t.Errorf("ReadFloat64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64WithEmptyFieldName(t *testing.T) {
	var value float64 = 22.43
	var expectedRet float64 = 0
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "point", FLOAT64, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64("point", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret, _ := mpr.ReadFloat64("")
	if expectedRet != ret {
		t.Errorf("ReadFloat64() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64WithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet bool = true
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "age", BOOL, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBool("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	_, err := mpr.ReadFloat64("age")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadFloat64() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadUTF(t *testing.T) {
	var expectedRet string = "Furkan"
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "engineer", UTF, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUTF("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadUTF("engineer")
	if ret != expectedRet {
		t.Errorf("ReadUTF() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUTFWithEmptyFieldName(t *testing.T) {
	var value string = "Furkan"
	var expectedRet string = ""
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "engineer", UTF, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUTF("engineer", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadUTF("")
	if ret != expectedRet {
		t.Errorf("ReadUTF() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUTFWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet int16 = 12
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "engineer", INT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	_, err := mpr.ReadUTF("engineer")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadUTF() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadPortable(t *testing.T) {
	var expectedRet Portable = &student{10, 22, "Furkan Şenharputlu"}
	config := NewSerializationConfig()
	config.AddPortableFactory(2, &PortableFactory1{})
	classDef := NewClassDefinitionImpl(2, 1, 3)
	service := NewSerializationService(config)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "engineer", PORTABLE, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, service, false)
	serializer, _ := service.FindSerializerFor(expectedRet)
	pw := NewDefaultPortableWriter(serializer.(*PortableSerializer), o, classDef)
	pw.WritePortable("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, service, false)

	mpr := NewMorphingPortableReader(serializer.(*PortableSerializer), i, pw.classDefinition)
	ret, _ := mpr.ReadPortable("engineer")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortable() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadPortableWithEmptyFieldName(t *testing.T) {
	var value Portable = &student{10, 22, "Furkan Şenharputlu"}
	var expectedRet Portable
	config := NewSerializationConfig()
	config.AddPortableFactory(2, &PortableFactory1{})
	classDef := NewClassDefinitionImpl(2, 1, 3)
	service := NewSerializationService(config)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "engineer", PORTABLE, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, service, false)
	serializer, _ := service.FindSerializerFor(value)
	pw := NewDefaultPortableWriter(serializer.(*PortableSerializer), o, classDef)
	pw.WritePortable("engineer", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, service, false)

	mpr := NewMorphingPortableReader(serializer.(*PortableSerializer), i, pw.classDefinition)
	ret, _ := mpr.ReadPortable("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortable() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadPortableWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet int16 = 12
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "engineer", INT16, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	_, err := mpr.ReadPortable("engineer")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadPortable() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadByteArray(t *testing.T) {
	var expectedRet []byte = []byte{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "types", BYTE_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByteArray("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadByteArray("types")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadByteArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadByteArrayWithEmptyFieldName(t *testing.T) {
	var value []byte = []byte{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	var expectedRet []byte
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "types", BYTE_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByteArray("types", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadByteArray("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadByteArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadByteArrayWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet []int32 = []int32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "types", INT32_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	_, err := mpr.ReadByteArray("types")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadByteArray() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadBoolArray(t *testing.T) {
	var expectedRet []bool = []bool{true, true, false, true, false, false, false, true, false, true, true}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "areReady", BOOL_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBoolArray("areReady", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadBoolArray("areReady")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadBoolArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadBoolArrayWithEmptyFieldName(t *testing.T) {
	var value []bool = []bool{true, true, false, true, false, false, false, true, false, true, true}
	var expectedRet []bool
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "areReady", BOOL_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBoolArray("areReady", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadBoolArray("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadBoolArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadBoolArrayWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet []int32 = []int32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "types", INT32_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	_, err := mpr.ReadBoolArray("types")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadBoolArray() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadUInt16Array(t *testing.T) {
	var expectedRet []uint16 = []uint16{'^', '%', '#', '!', '$'}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "scores", UINT16_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadUInt16Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadUInt16Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUInt16ArrayWithEmptyFieldName(t *testing.T) {
	var value []uint16 = []uint16{'^', '%', '#', '!', '$'}
	var expectedRet []uint16
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "scores", UINT16_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16Array("scores", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadUInt16Array("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadUInt16Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUInt16ArrayWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet []int32 = []int32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "types", INT32_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	_, err := mpr.ReadUInt16Array("types")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadUInt16Array() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadInt16Array(t *testing.T) {
	var expectedRet []int16 = []int16{9432, 12, 34, 126, 7, 343, 2, 0, 1120, 222, 440}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "scores", INT16_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadInt16Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt16Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt16ArrayWithEmptyFieldName(t *testing.T) {
	var value []int16 = []int16{9432, 12, 34, 126, 7, 343, 2, 0, 1120, 222, 440}
	var expectedRet []int16
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "scores", INT16_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16Array("scores", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadInt16Array("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt16Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt16ArrayWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet []int32 = []int32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "types", INT32_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	_, err := mpr.ReadInt16Array("types")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadInt16Array() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadInt32Array(t *testing.T) {
	var expectedRet []int32 = []int32{9432, 12, 34, 6123, 45367, 31341, 43142, 78690, 16790, 362, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "scores", INT32_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadInt32Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt32Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32ArrayWithEmptyFieldName(t *testing.T) {
	var value []int32 = []int32{9432, 12, 34, 6123, 45367, 31341, 43142, 78690, 16790, 362, 0}
	var expectedRet []int32
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "scores", INT32_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("scores", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadInt32Array("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt32Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt32ArrayWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet []int64 = []int64{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "types", INT64_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	_, err := mpr.ReadInt32Array("types")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadInt32Array() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadInt64Array(t *testing.T) {
	var expectedRet []int64 = []int64{9412332, 929812, 34, 61223493, 4523367, 31235341, 46423142, 78690, 16790, 3662, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "scores", INT64_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadInt64Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt64Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64ArrayWithEmptyFieldName(t *testing.T) {
	var value []int64 = []int64{9412332, 929812, 34, 61223493, 4523367, 31235341, 46423142, 78690, 16790, 3662, 0}
	var expectedRet []int64
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "scores", INT64_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64Array("scores", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadInt64Array("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt64Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt64ArrayWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet []float32 = []float32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "types", FLOAT32_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	_, err := mpr.ReadInt64Array("types")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadInt64Array() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadFloat32Array(t *testing.T) {
	var expectedRet []float32 = []float32{12.1431, 1212.3, 34, 6123, 4.5367, 3.1341, 43.142, 786.90, 16.790, 3.62, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "longitude", FLOAT32_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32Array("longitude", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadFloat32Array("longitude")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadFloat32Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32ArrayWithEmptyFieldName(t *testing.T) {
	var value []float32 = []float32{12.1431, 1212.3, 34, 6123, 4.5367, 3.1341, 43.142, 786.90, 16.790, 3.62, 0}
	var expectedRet []float32
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "longitude", FLOAT32_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32Array("longitude", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadFloat32Array("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadFloat32Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat32ArrayWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet []float64 = []float64{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "types", FLOAT64_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	_, err := mpr.ReadFloat32Array("types")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadFloat32Array() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadFloat64Array(t *testing.T) {
	var expectedRet []float64 = []float64{12234.1431, 121092.3, 34, 6123, 499.5364327, 3.1323441, 43.142, 799986.90, 16.790, 3.9996342, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "longitude", FLOAT64_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64Array("longitude", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadFloat64Array("longitude")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadFloat64Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64ArrayWithEmptyFieldName(t *testing.T) {
	var value []float64 = []float64{12234.1431, 121092.3, 34, 6123, 499.5364327, 3.1323441, 43.142, 799986.90, 16.790, 3.9996342, 0}
	var expectedRet []float64
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "longitude", FLOAT64_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64Array("longitude", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadFloat64Array("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadFloat64Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadFloat64ArrayWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet []float32 = []float32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "types", FLOAT32_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	_, err := mpr.ReadFloat64Array("types")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadFloat64Array() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadUTFArray(t *testing.T) {
	var expectedRet []string = []string{"Furkan Şenharputlu", "こんにちは", "おはようございます", "今晩は"}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "words", UTF_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUTFArray("words", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadUTFArray("words")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadUTFArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUTFArrayWithEmptyFieldName(t *testing.T) {
	var value []string = []string{"Furkan Şenharputlu", "こんにちは", "おはようございます", "今晩は"}
	var expectedRet []string
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "words", UTF_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUTFArray("words", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret, _ := mpr.ReadUTFArray("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadUTFArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUTFArrayWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet []float64 = []float64{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "types", FLOAT64_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	_, err := mpr.ReadUTFArray("types")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadUTFArray() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadPortableArray(t *testing.T) {
	var expectedRet []Portable = []Portable{&student{10, 22, "Furkan Şenharputlu"}, &student{11, 20, "Jack Purcell"}}
	config := NewSerializationConfig()
	config.AddPortableFactory(2, &PortableFactory1{})
	classDef := NewClassDefinitionImpl(2, 1, 3)
	service := NewSerializationService(config)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "engineers", PORTABLE_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	serializer := NewPortableSerializer(service, config.PortableFactories(), 0)
	pw := NewDefaultPortableWriter(serializer, o, classDef)
	pw.WritePortableArray("engineers", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(serializer, i, pw.classDefinition)
	ret, _ := mpr.ReadPortableArray("engineers")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortableArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadPortableArrayWithEmptyFieldName(t *testing.T) {
	var value []Portable = []Portable{&student{10, 22, "Furkan Şenharputlu"}, &student{11, 20, "Jack Purcell"}}
	var expectedRet []Portable
	config := NewSerializationConfig()
	config.AddPortableFactory(2, &PortableFactory1{})
	classDef := NewClassDefinitionImpl(2, 1, 3)
	service := NewSerializationService(config)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "engineers", PORTABLE_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	serializer := NewPortableSerializer(service, config.PortableFactories(), 0)
	pw := NewDefaultPortableWriter(serializer, o, classDef)
	pw.WritePortableArray("engineers", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(serializer, i, pw.classDefinition)
	ret, _ := mpr.ReadPortableArray("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortableArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadPortableArrayWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet []float64 = []float64{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(NewFieldDefinitionImpl(0, "types", FLOAT64_ARRAY, classDef.FactoryId(), classDef.ClassId(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	_, err := mpr.ReadPortableArray("types")
	if _, ok := err.(*core.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadPortableArray() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestNewMorphingPortableReader(t *testing.T) {
	s := &student{10, 22, "Furkan Şenharputlu"}
	config := NewSerializationConfig()
	config.AddPortableFactory(2, &PortableFactory2{})
	service := NewSerializationService(config)
	data, _ := service.ToData(s)

	service.serializationConfig.SetPortableVersion(1)
	expectedRet := &student2{10, 22, "Furkan Şenharputlu"}
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("MorphingPortableReader failed")
	}
}
