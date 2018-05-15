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
	"reflect"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization/classdef"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestDefaultPortableReader_ReadByte(t *testing.T) {
	var expectedRet byte = 12
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "type", classdef.TypeByte,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("type", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadByte("type")
	if expectedRet != ret {
		t.Errorf("ReadByte() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadBool(t *testing.T) {
	var expectedRet = false
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "isReady", classdef.TypeBool,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBool("isReady", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadBool("isReady")

	if expectedRet != ret {
		t.Errorf("ReadBool() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadUInt16(t *testing.T) {
	var expectedRet uint16 = 'E'
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "char", classdef.TypeUint16,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("char", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadUInt16("char")
	if expectedRet != ret {
		t.Errorf("ReadUInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadInt16(t *testing.T) {
	var expectedRet int16 = 22
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt16,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadInt16("age")
	if expectedRet != ret {
		t.Errorf("ReadInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadInt32(t *testing.T) {
	var expectedRet int32 = 22
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "age", classdef.TypeInt32,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadInt32("age")
	if expectedRet != ret {
		t.Errorf("ReadInt32() returns %d expected %d", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadInt64(t *testing.T) {
	var expectedRet int64 = 1000
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "score", classdef.TypeInt64,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64("score", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadInt64("score")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadFloat32(t *testing.T) {
	var expectedRet float32 = 18.2347123
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "rate", classdef.TypeFloat32,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32("rate", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadFloat32("rate")
	if expectedRet != ret {
		t.Errorf("ReadFloat32() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadFloat64(t *testing.T) {
	var expectedRet = 19.23433747
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "velocity", classdef.TypeFloat64,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64("velocity", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadFloat64("velocity")
	if expectedRet != ret {
		t.Errorf("ReadFloat64() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadUTF(t *testing.T) {
	var expectedRet string
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineer", classdef.TypeUTF,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUTF("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadUTF("engineer")
	if ret != expectedRet {
		t.Errorf("ReadUTF() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadPortable(t *testing.T) {
	var expectedRet serialization.Portable = &student{10, 22, "Furkan Şenharputlu"}
	config := config.NewSerializationConfig()
	config.AddPortableFactory(2, &portableFactory1{})
	classDef := classdef.NewClassDefinitionImpl(2, 1, 3)
	service, _ := NewSerializationService(config)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineer", classdef.TypePortable,
		classDef.FactoryID(), classDef.ClassID(), 0))

	o := NewPositionalObjectDataOutput(0, service, false)
	serializer, _ := service.FindSerializerFor(expectedRet)
	pw := NewDefaultPortableWriter(serializer.(*PortableSerializer), o, classDef)
	pw.WritePortable("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, service, false)

	pr := NewDefaultPortableReader(serializer.(*PortableSerializer), i, pw.classDefinition)
	ret, _ := pr.ReadPortable("engineer")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortable() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadNilPortable(t *testing.T) {
	var expectedRet serialization.Portable
	config := config.NewSerializationConfig()
	config.AddPortableFactory(2, &portableFactory1{})
	classDef := classdef.NewClassDefinitionImpl(2, 1, 3)
	service, _ := NewSerializationService(config)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineer", classdef.TypePortable,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, service, false)

	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteNilPortable("engineer", 2, 1)
	i := NewObjectDataInput(o.ToBuffer(), 0, service, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadPortable("engineer")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortable() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadByteArray(t *testing.T) {
	var expectedRet = []byte{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "types", classdef.TypeByteArray,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByteArray("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadByteArray("types")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadByteArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadBoolArray(t *testing.T) {
	var expectedRet = []bool{true, true, false, true, false, false, false, true, false, true, true}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "areReady", classdef.TypeBoolArray,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBoolArray("areReady", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadBoolArray("areReady")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadBoolArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadUInt16Array(t *testing.T) {
	var expectedRet = []uint16{'^', '%', '#', '!', '$'}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "scores", classdef.TypeUint16Array,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadUInt16Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadUInt16Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadInt16Array(t *testing.T) {
	var expectedRet = []int16{9432, 12, 34, 126, 7, 343, 2, 0, 1120, 222, 440}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "scores", classdef.TypeInt16Array,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadInt16Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt16Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadInt32Array(t *testing.T) {
	var expectedRet = []int32{9432, 12, 34, 6123, 45367, 31341, 43142, 78690, 16790, 362, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "scores", classdef.TypeInt32Array,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadInt32Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt32Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadInt64Array(t *testing.T) {
	var expectedRet = []int64{9412332, 929812, 34, 61223493, 4523367, 31235341, 46423142, 78690, 16790, 3662, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "scores", classdef.TypeInt64Array,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadInt64Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt64Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadFloat32Array(t *testing.T) {
	var expectedRet = []float32{12.1431, 1212.3, 34, 6123, 4.5367, 3.1341, 43.142, 786.90, 16.790, 3.62, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "longitude", classdef.TypeFloat32Array,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32Array("longitude", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadFloat32Array("longitude")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadFloat32Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadFloat64Array(t *testing.T) {
	var expectedRet = []float64{12234.1431, 121092.3, 34, 6123, 499.5364327, 3.1323441, 43.142, 799986.90, 16.790, 3.9996342, 0}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "longitude", classdef.TypeFloat64Array,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64Array("longitude", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadFloat64Array("longitude")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadFloat64Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadUTFArray(t *testing.T) {
	var expectedRet = []string{"Furkan Şenharputlu", "こんにちは", "おはようございます", "今晩は"}
	classDef := classdef.NewClassDefinitionImpl(1, 2, 3)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "words", classdef.TypeUTFArray,
		classDef.FactoryID(), classDef.ClassID(), 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUTFArray("words", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadUTFArray("words")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadUTFArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadPortableArray(t *testing.T) {
	var expectedRet = []serialization.Portable{&student{10, 22, "Furkan Şenharputlu"},
		&student{11, 20, "Jack Purcell"}}
	config := config.NewSerializationConfig()
	config.AddPortableFactory(2, &portableFactory1{})
	classDef := classdef.NewClassDefinitionImpl(2, 1, 3)
	service, _ := NewSerializationService(config)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineers", classdef.TypePortableArray,
		classDef.FactoryID(), classDef.ClassID(), 0))

	o := NewPositionalObjectDataOutput(0, nil, false)
	serializer := NewPortableSerializer(service, config.PortableFactories(), 0)
	pw := NewDefaultPortableWriter(serializer, o, classDef)
	pw.WritePortableArray("engineers", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(serializer, i, pw.classDefinition)
	ret, _ := pr.ReadPortableArray("engineers")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortableArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_NilObjects(t *testing.T) {
	var expectedRet serialization.Portable
	config := config.NewSerializationConfig()
	config.AddPortableFactory(2, &portableFactory1{})
	classDef := classdef.NewClassDefinitionImpl(2, 1, 3)
	service, _ := NewSerializationService(config)
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(0, "engineer", classdef.TypePortable,
		classDef.FactoryID(), classDef.ClassID(), 3))
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(1, "name", classdef.TypeUTF,
		classDef.FactoryID(), classDef.ClassID(), 3))
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(2, "a1", classdef.TypeByteArray,
		classDef.FactoryID(), classDef.ClassID(), 3))
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(3, "a2", classdef.TypeBoolArray,
		classDef.FactoryID(), classDef.ClassID(), 3))
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(4, "a3", classdef.TypeUint16Array,
		classDef.FactoryID(), classDef.ClassID(), 3))
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(5, "a4", classdef.TypeInt16Array,
		classDef.FactoryID(), classDef.ClassID(), 3))
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(6, "a5", classdef.TypeInt32Array,
		classDef.FactoryID(), classDef.ClassID(), 3))
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(7, "a6", classdef.TypeInt64Array,
		classDef.FactoryID(), classDef.ClassID(), 3))
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(8, "a7", classdef.TypeFloat32Array,
		classDef.FactoryID(), classDef.ClassID(), 3))
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(9, "a8", classdef.TypeFloat64Array,
		classDef.FactoryID(), classDef.ClassID(), 3))
	classDef.AddFieldDefinition(classdef.NewFieldDefinitionImpl(10, "a9", classdef.TypeUTFArray,
		classDef.FactoryID(), classDef.ClassID(), 3))

	o := NewPositionalObjectDataOutput(0, service, false)

	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteNilPortable("engineer", 2, 1)
	pw.WriteUTF("name", "")
	pw.WriteByteArray("a1", nil)
	pw.WriteBoolArray("a2", nil)
	pw.WriteUInt16Array("a3", nil)
	pw.WriteInt16Array("a4", nil)
	pw.WriteInt32Array("a5", nil)
	pw.WriteInt64Array("a6", nil)
	pw.WriteFloat32Array("a7", nil)
	pw.WriteFloat64Array("a8", nil)
	pw.WriteUTFArray("a9", nil)
	i := NewObjectDataInput(o.ToBuffer(), 0, service, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret, _ := pr.ReadPortable("engineer")
	ret1, _ := pr.ReadUTF("name")
	ret2, _ := pr.ReadByteArray("a1")
	ret3, _ := pr.ReadBoolArray("a2")
	ret4, _ := pr.ReadUInt16Array("a3")
	ret5, _ := pr.ReadInt16Array("a4")
	ret6, _ := pr.ReadInt32Array("a5")
	ret7, _ := pr.ReadInt64Array("a6")
	ret8, _ := pr.ReadFloat32Array("a7")
	ret9, _ := pr.ReadFloat64Array("a8")
	ret10, _ := pr.ReadUTFArray("a9")

	if ret != nil || ret1 != "" || ret2 != nil || ret3 != nil || ret4 != nil || ret5 != nil ||
		ret6 != nil || ret7 != nil || ret8 != nil || ret9 != nil || ret10 != nil {
		t.Errorf("ReadPortable() returns %v expected %v", ret, expectedRet)
	}
}
