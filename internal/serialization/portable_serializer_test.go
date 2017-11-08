package serialization

import (
	. "github.com/hazelcast/go-client/config"
	. "github.com/hazelcast/go-client/internal/serialization/api"
	"reflect"
	"testing"
)

type PortableFactory1 struct {
}

func (*PortableFactory1) Create(classId int32) Portable {
	if classId == 1 {
		return &student{}
	} else if classId == 2 {
		return &fake{}
	}
	return nil
}

type student struct {
	id   int16
	age  int32
	name string
}

func (*student) FactoryId() int32 {
	return 2
}

func (*student) ClassId() int32 {
	return 1
}

func (s *student) WritePortable(writer PortableWriter) {
	writer.WriteInt16("id", s.id)
	writer.WriteInt32("age", s.age)
	writer.WriteUTF("name", s.name)
}

func (s *student) ReadPortable(reader PortableReader) {
	s.id, _ = reader.ReadInt16("id")
	s.age, _ = reader.ReadInt32("age")
	s.name, _ = reader.ReadUTF("name")
}

func TestPortableSerializer(t *testing.T) {

	config := NewSerializationConfig()
	config.AddPortableFactory(2, &PortableFactory1{})
	service := NewSerializationService(config, make(map[int32]IdentifiedDataSerializableFactory))
	expectedRet := &student{10, 22, "Furkan Şenharputlu"}
	data, _ := service.ToData(expectedRet)
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(ret, expectedRet) {
		t.Errorf("ReadObject() returns %v expected %v", ret, expectedRet)
	}

}

type fake struct {
	byt          byte
	boo          bool
	ui16         uint16
	i16          int16
	i32          int32
	i64          int64
	f32          float32
	f64          float64
	utf          string
	portable     Portable
	byt_arr      []byte
	boo_arr      []bool
	ui16_arr     []uint16
	i16_arr      []int16
	i32_arr      []int32
	i64_arr      []int64
	f32_arr      []float32
	f64_arr      []float64
	utf_arr      []string
	portable_arr []Portable
}

func (*fake) FactoryId() int32 {
	return 2
}

func (*fake) ClassId() int32 {
	return 2
}

func (f *fake) WritePortable(writer PortableWriter) {
	writer.WriteByte("byt", f.byt)
	writer.WriteBool("boo", f.boo)
	writer.WriteUInt16("ui16", f.ui16)
	writer.WriteInt16("i16", f.i16)
	writer.WriteInt32("i32", f.i32)
	writer.WriteInt64("i64", f.i64)
	writer.WriteFloat32("f32", f.f32)
	writer.WriteFloat64("f64", f.f64)
	writer.WriteUTF("utf", f.utf)
	writer.WritePortable("portable", f.portable)
	writer.WriteByteArray("byt_arr", f.byt_arr)
	writer.WriteBoolArray("boo_arr", f.boo_arr)
	writer.WriteUInt16Array("ui16_arr", f.ui16_arr)
	writer.WriteInt16Array("i16_arr", f.i16_arr)
	writer.WriteInt32Array("i32_arr", f.i32_arr)
	writer.WriteInt64Array("i64_arr", f.i64_arr)
	writer.WriteFloat32Array("f32_arr", f.f32_arr)
	writer.WriteFloat64Array("f64_arr", f.f64_arr)
	writer.WriteUTFArray("utf_arr", f.utf_arr)
	writer.WritePortableArray("portable_arr", f.portable_arr)

}

func (f *fake) ReadPortable(reader PortableReader) {
	f.byt, _ = reader.ReadByte("byt")
	f.boo, _ = reader.ReadBool("boo")
	f.ui16, _ = reader.ReadUInt16("ui16")
	f.i16, _ = reader.ReadInt16("i16")
	f.i32, _ = reader.ReadInt32("i32")
	f.i64, _ = reader.ReadInt64("i64")
	f.f32, _ = reader.ReadFloat32("f32")
	f.f64, _ = reader.ReadFloat64("f64")
	f.utf, _ = reader.ReadUTF("utf")
	f.portable, _ = reader.ReadPortable("portable")
	f.byt_arr, _ = reader.ReadByteArray("byt_arr")
	f.boo_arr, _ = reader.ReadBoolArray("boo_arr")
	f.ui16_arr, _ = reader.ReadUInt16Array("ui16_arr")
	f.i16_arr, _ = reader.ReadInt16Array("i16_arr")
	f.i32_arr, _ = reader.ReadInt32Array("i32_arr")
	f.i64_arr, _ = reader.ReadInt64Array("i64_arr")
	f.f32_arr, _ = reader.ReadFloat32Array("f32_arr")
	f.f64_arr, _ = reader.ReadFloat64Array("f64_arr")
	f.utf_arr, _ = reader.ReadUTFArray("utf_arr")
	f.portable_arr, _ = reader.ReadPortableArray("portable_arr")
}

func TestPortableSerializer2(t *testing.T) {

	config := NewSerializationConfig()
	config.AddPortableFactory(2, &PortableFactory1{})
	service := NewSerializationService(config, make(map[int32]IdentifiedDataSerializableFactory))

	var byt byte = 255
	var boo bool = true
	var ui16 uint16 = 65535
	var i16 int16 = -32768
	var i32 int32 = -2147483648
	var i64 int64 = -9223372036854775808
	var f32 float32 = -3.4E+38
	var f64 float64 = -1.7E+308
	var utf string = "Günaydın, こんにちは"
	var portable Portable = &student{10, 22, "Furkan Şenharputlu"}
	var byt_arr []byte = []byte{127, 128, 255, 0, 4, 6, 8, 121}
	var boo_arr []bool = []bool{true, true, false, true, false, false, false, true, false, true}
	var ui16_arr []uint16 = []uint16{65535, 65535, 65535, 1234, 23524, 13131, 9999}
	var i16_arr []int16 = []int16{-32768, -2222, 32767, 0}
	var i32_arr []int32 = []int32{-2147483648, 234123, 13123, 13144, 14134, 2147483647}
	var i64_arr []int64 = []int64{-9223372036854775808, 1231231231231, 315253647, 255225, 9223372036854775807}
	var f32_arr []float32 = []float32{-3.4E+38, 12.344, 21.2646, 3.4E+38}
	var f64_arr []float64 = []float64{-1.7E+308, 1213.2342, 45345.9887, 1.7E+308}
	var utf_arr []string = []string{"こんにちは", "ilköğretim", "FISTIKÇIŞAHAP"}
	var portable_arr []Portable = []Portable{&student{10, 22, "Furkan Şenharputlu"}, &student{2, 20, "Micheal Micheal"}}

	expectedRet := &fake{byt, boo, ui16, i16, i32, i64, f32, f64, utf, portable,
		byt_arr, boo_arr, ui16_arr, i16_arr, i32_arr, i64_arr, f32_arr, f64_arr, utf_arr, portable_arr}
	data, _ := service.ToData(expectedRet)
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(ret, expectedRet) {
		t.Errorf("ReadObject() returns %v expected %v", ret, expectedRet)
	}

}
