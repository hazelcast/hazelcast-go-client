// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package internal

import (
	"errors"
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/internal/classdef"
	"github.com/stretchr/testify/assert"
)

func TestMorphingPortableReader_ReadByte(t *testing.T) {
	var expectedRet byte = 12
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "type", classdef.TypeByte,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var value byte = 12
	var expectedRet byte
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "type", classdef.TypeByte,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("type", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadByte("")
	if expectedRet != ret {
		t.Errorf("ReadByte() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadByteWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet = true
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "type", classdef.TypeBool,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBool("type", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	mpr.ReadByte("type")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadByte() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadBool(t *testing.T) {
	var expectedRet = true
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "isReady", classdef.TypeBool,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var value = true
	var expectedRet = false
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "isReady", classdef.TypeBool,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBool("isReady", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadBool("")

	if expectedRet != ret {
		t.Errorf("ReadBool() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadBoolWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet byte = 23
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "type", classdef.TypeByte,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("type", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	mpr.ReadBool("type")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadBool() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadUInt16(t *testing.T) {
	var expectedRet uint16 = 'E'
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "char", classdef.TypeUint16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "char", classdef.TypeUint16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var expectedRet int16 = 23
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "char", classdef.TypeInt16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("char", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewMorphingPortableReader(nil, i, classDef)
	pr.ReadUInt16("char")
	if _, ok := pr.Error().(*core.HazelcastSerializationError); !ok || pr.Error() == nil {
		t.Error("ReadUInt16() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadInt16FromByte(t *testing.T) {
	var expectedRet int16 = 22
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeByte,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var value int16 = 22
	var expectedRet int16
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	ret := mpr.ReadInt16("")
	if expectedRet != ret {
		t.Errorf("ReadInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadInt16WithIncompatibleClassChangeError(t *testing.T) {
	var value int64 = 22
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt64,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64("age", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	mpr.ReadInt16("age")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadInt16() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadInt32FromByte(t *testing.T) {
	var expectedRet int32 = 22
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeByte,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "letter", classdef.TypeUint16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt32,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var value int64 = 22
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt64,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64("age", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	mpr.ReadInt32("age")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadInt32() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadInt64FromByte(t *testing.T) {
	var expectedRet int64 = 22
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeByte,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeUint16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt32,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt64,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeUint16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var value float32 = 22.23
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeFloat32,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32("age", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	mpr.ReadInt64("age")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadInt64() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadFloat32FromByte(t *testing.T) {
	var expectedRet byte = 22
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeByte,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeUint16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt32,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeFloat32,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeFloat32,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var expectedRet = 22.5
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeFloat64,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	mpr.ReadFloat32("age")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadFloat32() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadFloat64FromByte(t *testing.T) {
	var expectedRet byte = 22
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "point", classdef.TypeByte,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "point", classdef.TypeUint16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "point", classdef.TypeInt16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "point", classdef.TypeInt32,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "point", classdef.TypeInt64,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "point", classdef.TypeFloat32,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "point", classdef.TypeFloat64,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "point", classdef.TypeFloat64,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var expectedRet = true
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeBool,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBool("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, classDef)
	mpr.ReadFloat64("age")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadFloat64() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadUTF(t *testing.T) {
	var expectedRet = "Furkan"
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineer", classdef.TypeUTF,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUTF("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadUTF("engineer")
	if ret != expectedRet {
		t.Errorf("ReadUTF() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUTFWithEmptyFieldName(t *testing.T) {
	var value = "Furkan"
	var expectedRet string
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineer", classdef.TypeUTF,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUTF("engineer", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadUTF("")
	if ret != expectedRet {
		t.Errorf("ReadUTF() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUTFWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet int16 = 12
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineer", classdef.TypeInt16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	mpr.ReadUTF("engineer")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadUTF() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadPortable(t *testing.T) {
	var expectedRet = &student{10, 22, "Furkan Şenharputlu"}
	config := serialization.NewConfig()
	config.AddPortableFactory(2, &portableFactory1{})
	classDef := classdef.NewClassDefinitionImpl(2, 1, 3)
	service, _ := NewService(config)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineer", classdef.TypePortable,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))

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
	var value serialization.Portable = &student{10, 22, "Furkan Şenharputlu"}
	var expectedRet serialization.Portable
	config := serialization.NewConfig()
	config.AddPortableFactory(2, &portableFactory1{})
	classDef := classdef.NewClassDefinitionImpl(2, 1, 3)
	service, _ := NewService(config)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineer", classdef.TypePortable,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))

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
	var expectedRet int16 = 12
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineer", classdef.TypeInt16,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	mpr.ReadPortable("engineer")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadPortable() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadByteArray(t *testing.T) {
	var expectedRet = []byte{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeByteArray,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeByteArray,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var expectedRet = []int32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeInt32Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	mpr.ReadByteArray("types")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadByteArray() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadBoolArray(t *testing.T) {
	var expectedRet = []bool{true, true, false, true, false, false, false, true, false, true, true}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "areReady", classdef.TypeBoolArray,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "areReady", classdef.TypeBoolArray,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var expectedRet = []int32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeInt32Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	mpr.ReadBoolArray("types")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadBoolArray() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadUInt16Array(t *testing.T) {
	var expectedRet = []uint16{'^', '%', '#', '!', '$'}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "scores", classdef.TypeUint16Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "scores", classdef.TypeUint16Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var expectedRet = []int32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeInt32Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	mpr.ReadUInt16Array("types")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadUInt16Array() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadInt16Array(t *testing.T) {
	var expectedRet = []int16{9432, 12, 34, 126, 7, 343, 2, 0, 1120, 222, 440}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "scores", classdef.TypeInt16Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "scores", classdef.TypeInt16Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var expectedRet = []int32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeInt32Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	mpr.ReadInt16Array("types")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadInt16Array() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadInt32Array(t *testing.T) {
	var expectedRet = []int32{9432, 12, 34, 6123, 45367, 31341, 43142, 78690, 16790, 362, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "scores", classdef.TypeInt32Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "scores", classdef.TypeInt32Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var expectedRet = []int64{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeInt64Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	mpr.ReadInt32Array("types")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadInt32Array() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadInt64Array(t *testing.T) {
	var expectedRet = []int64{9412332, 929812, 34, 61223493, 4523367, 31235341, 46423142, 78690, 16790, 3662, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "scores", classdef.TypeInt64Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "scores", classdef.TypeInt64Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var expectedRet = []float32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeFloat32Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	mpr.ReadInt64Array("types")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadInt64Array() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadFloat32Array(t *testing.T) {
	var expectedRet = []float32{12.1431, 1212.3, 34, 6123, 4.5367, 3.1341, 43.142, 786.90, 16.790, 3.62, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "longitude", classdef.TypeFloat32Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "longitude", classdef.TypeFloat32Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var expectedRet = []float64{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeFloat64Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	mpr.ReadFloat32Array("types")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadFloat32Array() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadFloat64Array(t *testing.T) {
	var expectedRet = []float64{12234.1431, 121092.3, 34, 6123, 499.5364327, 3.1323441, 43.142, 799986.90, 16.790, 3.9996342, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "longitude", classdef.TypeFloat64Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "longitude", classdef.TypeFloat64Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
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
	var expectedRet = []float32{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeFloat32Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	mpr.ReadFloat64Array("types")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadFloat64Array() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadUTFArray(t *testing.T) {
	var expectedRet = []string{"Furkan Şenharputlu", "こんにちは", "おはようございます", "今晩は"}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "words", classdef.TypeUTFArray,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUTFArray("words", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadUTFArray("words")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadUTFArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUTFArrayWithEmptyFieldName(t *testing.T) {
	var value = []string{"Furkan Şenharputlu", "こんにちは", "おはようございます", "今晩は"}
	var expectedRet []string
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "words", classdef.TypeUTFArray,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUTFArray("words", value)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	ret := mpr.ReadUTFArray("")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadUTFArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUTFArrayWithIncompatibleClassChangeError(t *testing.T) {
	var expectedRet = []float64{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeFloat64Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	mpr.ReadUTFArray("types")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadUTFArray() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadPortableArray(t *testing.T) {
	var expectedRet = []serialization.Portable{&student{10, 22, "Furkan Şenharputlu"},
		&student{11, 20, "Jack Purcell"}}
	config := serialization.NewConfig()
	config.AddPortableFactory(2, &portableFactory1{})
	classDef := classdef.NewClassDefinitionImpl(2, 1, 3)
	service, _ := NewService(config)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineers", classdef.TypePortableArray,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	serializer := NewPortableSerializer(service, config.PortableFactories(), 0)
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
	var value = []serialization.Portable{&student{10, 22, "Furkan Şenharputlu"},
		&student{11, 20, "Jack Purcell"}}
	var expectedRet []serialization.Portable
	config := serialization.NewConfig()
	config.AddPortableFactory(2, &portableFactory1{})
	classDef := classdef.NewClassDefinitionImpl(2, 1, 3)
	service, _ := NewService(config)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineers", classdef.TypePortableArray,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	serializer := NewPortableSerializer(service, config.PortableFactories(), 0)
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
	var expectedRet = []float64{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeFloat64Array,
		classDef.FactoryID(), classDef.ClassID(), classDef.Version()))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64Array("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	mpr := NewMorphingPortableReader(nil, i, pw.classDefinition)
	mpr.ReadPortableArray("types")
	if _, ok := mpr.Error().(*core.HazelcastSerializationError); !ok || mpr.Error() == nil {
		t.Error("ReadPortableArray() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestNewMorphingPortableReader(t *testing.T) {
	s := &student{10, 22, "Furkan Şenharputlu"}
	config := serialization.NewConfig()
	config.AddPortableFactory(2, &portableFactory2{})
	service, _ := NewService(config)

	data, _ := service.ToData(s)

	service.serializationConfig.SetPortableVersion(1)
	expectedRet := &student2{10, 22, "Furkan Şenharputlu"}
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Error("MorphingPortableReader failed")
	}
}

func TestMorphingPortableReader_SameErrorIsReturned(t *testing.T) {

	pr := &MorphingPortableReader{&DefaultPortableReader{}}
	expectedError := errors.New("error")
	pr.err = expectedError
	pr.ReadBool("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadByte("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadInt64Array("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadInt64("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadInt16Array("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadInt16Array("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadInt32Array("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadInt32("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadFloat64Array("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadFloat64("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadUTF("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadByteArray("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadBoolArray("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadUInt16Array("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadUInt16("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadUTFArray("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadPortable("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadPortableArray("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadInt16("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadFloat32("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

	pr.ReadFloat32Array("dummy")
	assert.Error(t, pr.Error())
	assert.Equal(t, pr.Error(), expectedError)

}
