package serialization

import (
	. "github.com/hazelcast/go-client/config"
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/serialization"
	"reflect"
	"testing"
)

func TestMorphingPortableReader_createIncompatibleClassChangeError(t *testing.T) {
	var expectedRet int32 = 22
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "age", INT, classDef.factoryId, classDef.classId))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteInt32("age", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	classDef2 := NewClassDefinition(1, 2, 4)
	classDef2.addFieldDefinition(NewFieldDefinition(0, "age", FLOAT, classDef2.factoryId, classDef2.classId))
	mpr := NewMorphingPortableReader(nil, i, classDef2)
	_, err := mpr.ReadInt32("age")
	if _, ok := err.(*common.HazelcastSerializationError); !ok || err == nil {
		t.Errorf("ReadInt32() should return error type *common.HazelcastSerializationError but it does not return")
	}
}

func TestMorphingPortableReader_ReadByte(t *testing.T) {
	var expectedRet byte = 12
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "type", BYTE, classDef.factoryId, classDef.classId))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteByte("type", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	classDef2 := NewClassDefinition(1, 2, 4)
	classDef2.addFieldDefinition(NewFieldDefinition(0, "type", BYTE, classDef2.factoryId, classDef2.classId))
	mpr := NewMorphingPortableReader(nil, i, classDef2)
	ret, _ := mpr.ReadByte("type")
	if expectedRet != ret {
		t.Errorf("ReadByte() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadBool(t *testing.T) {
	var expectedRet bool = false
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "isReady", BOOLEAN, classDef.factoryId, classDef.classId))
	o := NewPositionalObjectDataOutput(0, nil, false)
	pw := NewDefaultPortableWriter(nil, o, classDef)
	pw.WriteBool("isReady", expectedRet)
	i := NewObjectDataInput(o.ToBuffer(), 0, nil, false)

	classDef2 := NewClassDefinition(1, 2, 4)
	classDef2.addFieldDefinition(NewFieldDefinition(0, "isReady", BOOLEAN, classDef2.factoryId, classDef2.classId))
	mpr := NewMorphingPortableReader(nil, i, classDef2)
	ret, _ := mpr.ReadBool("isReady")

	if expectedRet != ret {
		t.Errorf("ReadBool() returns %v expected %v", ret, expectedRet)
	}
}

func TestMorphingPortableReader_ReadUInt16(t *testing.T) {
	var expectedRet uint16 = 'E'
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "char", CHAR, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadInt16(t *testing.T) {
	var expectedRet int16 = 22
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "age", BYTE, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadInt32(t *testing.T) {
	var expectedRet int32 = 22
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "age", SHORT, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadInt64(t *testing.T) {
	var expectedRet int64 = 22
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "age", CHAR, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadFloat32(t *testing.T) {
	var expectedRet int16 = 22
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "age", SHORT, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadFloat64(t *testing.T) {
	var expectedRet float32 = 22.43
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "point", FLOAT, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadUTF(t *testing.T) {
	var expectedRet string = ""
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "engineer", UTF, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadPortable(t *testing.T) {
	var expectedRet Portable = &student{10, 22, "Furkan Şenharputlu"}
	config := NewSerializationConfig()
	config.AddPortableFactory(2, &PortableFactory1{})
	classDef := NewClassDefinition(2, 1, 3)
	service := NewSerializationService(config)
	classDef.addFieldDefinition(NewFieldDefinition(0, "engineer", PORTABLE, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadByteArray(t *testing.T) {
	var expectedRet []byte = []byte{9, 12, 34, 6, 7, 3, 2, 0, 10, 2, 0}
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "types", BYTE_ARRAY, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadBoolArray(t *testing.T) {
	var expectedRet []bool = []bool{true, true, false, true, false, false, false, true, false, true, true}
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "areReady", BOOLEAN_ARRAY, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadUInt16Array(t *testing.T) {
	var expectedRet []uint16 = []uint16{'^', '%', '#', '!', '$'}
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "scores", CHAR_ARRAY, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadInt16Array(t *testing.T) {
	var expectedRet []int16 = []int16{9432, 12, 34, 126, 7, 343, 2, 0, 1120, 222, 440}
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "scores", SHORT_ARRAY, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadInt32Array(t *testing.T) {
	var expectedRet []int32 = []int32{9432, 12, 34, 6123, 45367, 31341, 43142, 78690, 16790, 362, 0}
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "scores", INT_ARRAY, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadInt64Array(t *testing.T) {
	var expectedRet []int64 = []int64{9412332, 929812, 34, 61223493, 4523367, 31235341, 46423142, 78690, 16790, 3662, 0}
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "scores", LONG_ARRAY, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadFloat32Array(t *testing.T) {
	var expectedRet []float32 = []float32{12.1431, 1212.3, 34, 6123, 4.5367, 3.1341, 43.142, 786.90, 16.790, 3.62, 0}
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "longitude", FLOAT_ARRAY, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadFloat64Array(t *testing.T) {
	var expectedRet []float64 = []float64{12234.1431, 121092.3, 34, 6123, 499.5364327, 3.1323441, 43.142, 799986.90, 16.790, 3.9996342, 0}
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "longitude", DOUBLE_ARRAY, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadUTFArray(t *testing.T) {
	var expectedRet []string = []string{"Furkan Şenharputlu", "こんにちは", "おはようございます", "今晩は"}
	classDef := NewClassDefinition(1, 2, 3)
	classDef.addFieldDefinition(NewFieldDefinition(0, "words", UTF_ARRAY, classDef.factoryId, classDef.classId))
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

func TestMorphingPortableReader_ReadPortableArray(t *testing.T) {
	var expectedRet []Portable = []Portable{&student{10, 22, "Furkan Şenharputlu"}, &student{11, 20, "Jack Purcell"}}
	config := NewSerializationConfig()
	config.AddPortableFactory(2, &PortableFactory1{})
	classDef := NewClassDefinition(2, 1, 3)
	service := NewSerializationService(config)
	classDef.addFieldDefinition(NewFieldDefinition(0, "engineers", PORTABLE_ARRAY, classDef.factoryId, classDef.classId))
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
