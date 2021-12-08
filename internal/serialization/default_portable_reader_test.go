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
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestDefaultPortableReader_ReadByte(t *testing.T) {
	var expectedRet byte = 12
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "type", serialization.TypeByte,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("type", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadByte("type")
	if expectedRet != ret {
		t.Errorf("ReadByte() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadBool(t *testing.T) {
	expectedRet := false
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "isReady", serialization.TypeBool,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBool("isReady", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadBool("isReady")

	if expectedRet != ret {
		t.Errorf("ReadBool() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadUInt16(t *testing.T) {
	var expectedRet uint16 = 'E'
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "char", serialization.TypeUint16,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16("char", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadUInt16("char")
	if expectedRet != ret {
		t.Errorf("ReadUInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadInt16(t *testing.T) {
	var expectedRet int16 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt16,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadInt16("age")
	if expectedRet != ret {
		t.Errorf("ReadInt16() returns %d expected %d", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadInt32(t *testing.T) {
	var expectedRet int32 = 22
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "age", serialization.TypeInt32,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadInt32("age")
	if expectedRet != ret {
		t.Errorf("ReadInt32() returns %d expected %d", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadInt64(t *testing.T) {
	var expectedRet int64 = 1000
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "score", serialization.TypeInt64,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64("score", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadInt64("score")
	if expectedRet != ret {
		t.Errorf("ReadInt64() returns %d expected %d", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadFloat32(t *testing.T) {
	var expectedRet float32 = 18.2347123
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "rate", serialization.TypeFloat32,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32("rate", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadFloat32("rate")
	if expectedRet != ret {
		t.Errorf("ReadFloat32() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadFloat64(t *testing.T) {
	var expectedRet = 19.23433747
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "velocity", serialization.TypeFloat64,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64("velocity", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadFloat64("velocity")
	if expectedRet != ret {
		t.Errorf("ReadFloat64() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadString(t *testing.T) {
	var expectedRet string
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "engineer", serialization.TypeString,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteString("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadString("engineer")
	if ret != expectedRet {
		t.Errorf("ReadString() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadPortable(t *testing.T) {
	var expectedRet serialization.Portable = &student{
		id:   10,
		age:  22,
		name: "Furkan Şenharputlu",
	}
	config := &serialization.Config{}
	config.SetPortableFactories(&portableFactory1{})
	classDef := serialization.NewClassDefinition(2, 1, 3)
	service, _ := NewService(config)
	classDef.AddField(NewFieldDefinition(0, "engineer", serialization.TypePortable,
		classDef.FactoryID, classDef.ClassID, 0))

	o := NewPositionalObjectDataOutput(0, service, false)
	serializer, err := service.FindSerializerFor(expectedRet)
	if err != nil {
		t.Fatal(err)
	}
	pw := NewDefaultPortableWriter(serializer.(*PortableSerializer), o, classDef)
	pw.WritePortable("engineer", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, service, false)

	pr := NewDefaultPortableReader(serializer.(*PortableSerializer), i, pw.classDefinition)
	ret := pr.ReadPortable("engineer")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortable() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadNilPortable(t *testing.T) {
	var expectedRet serialization.Portable
	config := &serialization.Config{}
	config.SetPortableFactories(&portableFactory1{})
	classDef := serialization.NewClassDefinition(2, 1, 3)
	service, _ := NewService(config)
	classDef.AddField(NewFieldDefinition(0, "engineer", serialization.TypePortable,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, service, false)

	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteNilPortable("engineer", 2, 1)
	i := NewObjectDataInput(o.ToBuffer(), 0, service, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadPortable("engineer")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortable() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadByteArray(t *testing.T) {
	var expectedRet = []byte{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "types", serialization.TypeByteArray,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByteArray("types", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadByteArray("types")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadByteArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadBoolArray(t *testing.T) {
	var expectedRet = []bool{true, true, false, true, false, false, false, true, false, true, true}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "areReady", serialization.TypeBoolArray,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBoolArray("areReady", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadBoolArray("areReady")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadBoolArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadUInt16Array(t *testing.T) {
	var expectedRet = []uint16{'^', '%', '#', '!', '$'}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "scores", serialization.TypeUInt16Array,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteUInt16Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadUInt16Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadUInt16Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadInt16Array(t *testing.T) {
	var expectedRet = []int16{9432, 12, 34, 126, 7, 343, 2, 0, 1120, 222, 440}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "scores", serialization.TypeInt16Array,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt16Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadInt16Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt16Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadInt32Array(t *testing.T) {
	var expectedRet = []int32{9432, 12, 34, 6123, 45367, 31341, 43142, 78690, 16790, 362, 0}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "scores", serialization.TypeInt32Array,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadInt32Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt32Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadInt64Array(t *testing.T) {
	var expectedRet = []int64{9412332, 929812, 34, 61223493, 4523367, 31235341, 46423142, 78690, 16790, 3662, 0}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "scores", serialization.TypeInt64Array,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt64Array("scores", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadInt64Array("scores")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadInt64Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadFloat32Array(t *testing.T) {
	var expectedRet = []float32{12.1431, 1212.3, 34, 6123, 4.5367, 3.1341, 43.142, 786.90, 16.790, 3.62, 0}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "longitude", serialization.TypeFloat32Array,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat32Array("longitude", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadFloat32Array("longitude")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadFloat32Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadFloat64Array(t *testing.T) {
	var expectedRet = []float64{12234.1431, 121092.3, 34, 6123, 499.5364327, 3.1323441, 43.142, 799986.90, 16.790, 3.9996342, 0}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "longitude", serialization.TypeFloat64Array,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteFloat64Array("longitude", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadFloat64Array("longitude")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadFloat64Array() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadStringArray(t *testing.T) {
	var expectedRet = []string{"Furkan Şenharputlu", "こんにちは", "おはようございます", "今晩は"}
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, "words", serialization.TypeStringArray,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteStringArray("words", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadStringArray("words")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadStringArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadPortableArray(t *testing.T) {
	var expectedRet = []serialization.Portable{
		&student{id: 10, age: 22, name: "Furkan Şenharputlu"},
		&student{id: 11, age: 20, name: "Jack Purcell"},
	}
	config := &serialization.Config{}
	config.SetPortableFactories(&portableFactory1{})
	classDef := serialization.NewClassDefinition(2, 1, 3)
	service, _ := NewService(config)
	classDef.AddField(NewFieldDefinition(0, "engineers", serialization.TypePortableArray,
		classDef.FactoryID, classDef.ClassID, 0))
	o := NewPositionalObjectDataOutput(0, nil, false)
	serializer, err := NewPortableSerializer(service, config.PortableFactories(), 0)
	if err != nil {
		t.Fatal(err)
	}
	pw := NewDefaultPortableWriter(serializer, o, classDef)
	pw.WritePortableArray("engineers", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	pr := NewDefaultPortableReader(serializer, i, pw.classDefinition)
	ret := pr.ReadPortableArray("engineers")

	if !reflect.DeepEqual(expectedRet, ret) {
		t.Errorf("ReadPortableArray() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_NilObjects(t *testing.T) {
	var expectedRet serialization.Portable
	config := &serialization.Config{}
	config.SetPortableFactories(&portableFactory1{})
	classDef := serialization.NewClassDefinition(2, 1, 3)
	service, _ := NewService(config)
	classDef.AddField(NewFieldDefinition(0, "engineer", serialization.TypePortable,
		classDef.FactoryID, classDef.ClassID, 3))
	classDef.AddField(NewFieldDefinition(1, "name", serialization.TypeString,
		classDef.FactoryID, classDef.ClassID, 3))
	classDef.AddField(NewFieldDefinition(2, "a1", serialization.TypeByteArray,
		classDef.FactoryID, classDef.ClassID, 3))
	classDef.AddField(NewFieldDefinition(3, "a2", serialization.TypeBoolArray,
		classDef.FactoryID, classDef.ClassID, 3))
	classDef.AddField(NewFieldDefinition(4, "a3", serialization.TypeUInt16Array,
		classDef.FactoryID, classDef.ClassID, 3))
	classDef.AddField(NewFieldDefinition(5, "a4", serialization.TypeInt16Array,
		classDef.FactoryID, classDef.ClassID, 3))
	classDef.AddField(NewFieldDefinition(6, "a5", serialization.TypeInt32Array,
		classDef.FactoryID, classDef.ClassID, 3))
	classDef.AddField(NewFieldDefinition(7, "a6", serialization.TypeInt64Array,
		classDef.FactoryID, classDef.ClassID, 3))
	classDef.AddField(NewFieldDefinition(8, "a7", serialization.TypeFloat32Array,
		classDef.FactoryID, classDef.ClassID, 3))
	classDef.AddField(NewFieldDefinition(9, "a8", serialization.TypeFloat64Array,
		classDef.FactoryID, classDef.ClassID, 3))
	classDef.AddField(NewFieldDefinition(10, "a9", serialization.TypeStringArray,
		classDef.FactoryID, classDef.ClassID, 3))

	o := NewPositionalObjectDataOutput(0, service, false)

	pw := NewDefaultPortableWriter(nil, o, classDef)
	// XXX: unhandled error
	pw.WriteNilPortable("engineer", 2, 1)
	pw.WriteString("name", "")
	pw.WriteByteArray("a1", nil)
	pw.WriteBoolArray("a2", nil)
	pw.WriteUInt16Array("a3", nil)
	pw.WriteInt16Array("a4", nil)
	pw.WriteInt32Array("a5", nil)
	pw.WriteInt64Array("a6", nil)
	pw.WriteFloat32Array("a7", nil)
	pw.WriteFloat64Array("a8", nil)
	pw.WriteStringArray("a9", nil)
	i := NewObjectDataInput(o.ToBuffer(), 0, service, false)

	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadPortable("engineer")
	ret1 := pr.ReadString("name")
	ret2 := pr.ReadByteArray("a1")
	ret3 := pr.ReadBoolArray("a2")
	ret4 := pr.ReadUInt16Array("a3")
	ret5 := pr.ReadInt16Array("a4")
	ret6 := pr.ReadInt32Array("a5")
	ret7 := pr.ReadInt64Array("a6")
	ret8 := pr.ReadFloat32Array("a7")
	ret9 := pr.ReadFloat64Array("a8")
	ret10 := pr.ReadStringArray("a9")

	if ret != nil || ret1 != "" || ret2 != nil || ret3 != nil || ret4 != nil || ret5 != nil ||
		ret6 != nil || ret7 != nil || ret8 != nil || ret9 != nil || ret10 != nil {
		t.Errorf("ReadPortable() returns %v expected %v", ret, expectedRet)
	}
}

func TestDefaultPortableReader_ReadString_NonASCIIFieldName(t *testing.T) {
	// See: https://github.com/hazelcast/hazelcast/issues/17955#issuecomment-778152424
	service, err := NewService(&serialization.Config{})
	if err != nil {
		t.Fatal(err)
	}
	o := NewPositionalObjectDataOutput(0, service, false)
	cd := serialization.NewClassDefinition(2, 1, 3)
	if err = cd.AddStringField("şerıalızatıon"); err != nil {
		t.Fatal(err)
	}
	pw := NewDefaultPortableWriter(nil, o, cd)
	pw.WriteString("şerıalızatıon", "foo")
	i := NewObjectDataInput(o.ToBuffer(), 0, service, false)
	pr := NewDefaultPortableReader(nil, i, pw.classDefinition)
	ret := pr.ReadString("şerıalızatıon")
	assert.Equal(t, "foo", ret)
}

func TestDefaultPortableReader_PortableFieldsAfterRawData(t *testing.T) {
	const (
		writeErr                 = "cannot write Portable fields after getRawDataOutput() is called: hazelcast serialization error"
		readErr                  = "cannot read Portable fields after getRawDataInput() is called: hazelcast serialization error"
		portableFieldName        = "foo"
		portableFieldValue int64 = 42
	)
	classDef := serialization.NewClassDefinition(1, 2, 3)
	classDef.AddField(NewFieldDefinition(0, portableFieldName, serialization.TypeInt64,
		classDef.FactoryID, classDef.ClassID, 0))
	out := NewPositionalObjectDataOutput(0, nil, false)
	writer := NewDefaultPortableWriter(nil, out, classDef)
	writer.WriteInt64(portableFieldName, portableFieldValue)
	writer.GetRawDataOutput()
	t.Run("WritePortableField_AfterGetRawDataOutput", func(t *testing.T) {
		writerType := reflect.TypeOf(writer)
		for i := 0; i < writerType.NumMethod(); i++ {
			if method := writerType.Method(i); strings.HasPrefix(method.Name, "Write") {
				arg1 := reflect.ValueOf(writer)            // receiver type
				arg2 := reflect.ValueOf(portableFieldName) // non-empty fieldName string
				args := []reflect.Value{arg1, arg2}
				for argIdx := 2; argIdx < method.Type.NumIn(); argIdx++ {
					args = append(args, reflect.Zero(method.Type.In(argIdx)))
				}
				assert.PanicsWithError(t, writeErr, func() {
					method.Func.Call(args)
				})
			}
		}
	})
	in := NewObjectDataInput(out.ToBuffer(), 0, nil, false)
	reader := NewDefaultPortableReader(nil, in, writer.classDefinition)
	assert.Equal(t, portableFieldValue, reader.readInt64(portableFieldName))
	reader.GetRawDataInput()
	readerType := reflect.TypeOf(reader)
	for i := 0; i < readerType.NumMethod(); i++ {
		if method := readerType.Method(i); strings.HasPrefix(method.Name, "Read") {
			t.Run(fmt.Sprintf("ReadPortableField_AfterGetRawDataInput_%s", method.Name), func(t *testing.T) {
				assert.PanicsWithError(t, readErr, func() {
					arg1 := reflect.ValueOf(reader)            // receiver type
					arg2 := reflect.ValueOf(portableFieldName) // non-empty fieldName string
					method.Func.Call([]reflect.Value{arg1, arg2})
				})
			})
		}
	}
}

type rawPortable struct {
	id int32
}

func (*rawPortable) FactoryID() int32 {
	return 1
}

func (*rawPortable) ClassID() int32 {
	return 1
}

func (r *rawPortable) WritePortable(writer serialization.PortableWriter) {
	writer.GetRawDataOutput().WriteInt32(r.id)
}

func (r *rawPortable) ReadPortable(reader serialization.PortableReader) {
	r.id = reader.GetRawDataInput().ReadInt32()
}

type rawPortableFactory struct {
}

func (*rawPortableFactory) Create(classID int32) serialization.Portable {
	if classID == 1 {
		return &rawPortable{}
	}
	return nil
}

func (*rawPortableFactory) FactoryID() int32 {
	return 1
}

func TestNewPortableSerializer_RawData(t *testing.T) {
	config := &serialization.Config{}
	config.SetPortableFactories(&rawPortableFactory{})
	service, err := NewService(config)
	if err != nil {
		t.Fatal(err)
	}
	expected := &rawPortable{id: 42}
	data, err := service.ToData(expected)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := service.ToObject(data)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, actual)
}
