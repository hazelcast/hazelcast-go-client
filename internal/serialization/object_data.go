package serialization

import (
	"errors"
	. "github.com/hazelcast/go-client/internal/common"
	"unicode/utf8"
)

type ObjectDataOutput struct {
	buffer    []byte
	service   *SerializationService
	bigIndian bool
	position  int
}

func NewObjectDataOutput(length int, service *SerializationService, bigIndian bool) *ObjectDataOutput {
	return &ObjectDataOutput{make([]byte, length), service, bigIndian, 0}
}

func (o *ObjectDataOutput) Available() int {
	if o.buffer == nil {
		return 0
	} else {
		return len(o.buffer) - o.position
	}
}

func (o *ObjectDataOutput) EnsureAvailable(size int) {
	if o.Available() < size {
		add := make([]byte, size-o.Available())
		o.buffer = append(o.buffer, add...)
	}
}

func (o *ObjectDataOutput) WriteInt32(v int32) {
	o.EnsureAvailable(INT_SIZE_IN_BYTES)
	WriteInt32(o.buffer, v, o.position, o.bigIndian)
	o.position += INT_SIZE_IN_BYTES
}

func (o *ObjectDataOutput) WriteFloat32(v float32) {
	o.EnsureAvailable(FLOAT_SIZE_IN_BYTES)
	WriteFloat32(o.buffer, v, o.position, o.bigIndian)
	o.position += FLOAT_SIZE_IN_BYTES
}

func (o *ObjectDataOutput) WriteFloat64(v float64) {
	o.EnsureAvailable(DOUBLE_SIZE_IN_BYTES)
	WriteFloat64(o.buffer, v, o.position, o.bigIndian)
	o.position += DOUBLE_SIZE_IN_BYTES
}

func (o *ObjectDataOutput) WriteBool(v bool) {
	o.EnsureAvailable(BOOL_SIZE_IN_BYTES)
	WriteBool(o.buffer, v, o.position)
	o.position += BOOL_SIZE_IN_BYTES
}

func (o *ObjectDataOutput) WriteByte(v byte) {
	o.EnsureAvailable(BYTE_SIZE_IN_BYTES)
	WriteUInt8(o.buffer, o.position, v)
	o.position += BYTE_SIZE_IN_BYTES
}

func (o *ObjectDataOutput) WriteUTF(v string) {
	var length int32
	runes := []rune(v)
	length = int32(len(runes))
	o.WriteInt32(length)
	if (length > 0) {
		o.EnsureAvailable(len(v))
		for _, s := range runes {
			o.position += utf8.EncodeRune(o.buffer[o.position:], s)
		}
	}
}

func (o *ObjectDataOutput) WriteInt16(v int16) {
	o.EnsureAvailable(SHORT_SIZE_IN_BYTES)
	WriteInt16(o.buffer, v, o.position, o.bigIndian)
	o.position += SHORT_SIZE_IN_BYTES
}

func (o *ObjectDataOutput) WriteInt64(v int64) {
	o.EnsureAvailable(LONG_SIZE_IN_BYTES)
	WriteInt64(o.buffer, v, o.position, o.bigIndian)
	o.position += LONG_SIZE_IN_BYTES
}

func (o *ObjectDataOutput) WriteObject(object interface{}) {
	o.service.WriteObject(o, object)
}

func (o *ObjectDataOutput) WriteInt16Array(v []int16) {
	var length int32
	length = int32(len(v))
	o.WriteInt32(length)
	for j := int32(0); j < length; j++ {
		o.WriteInt16(v[j])
	}
}

func (o *ObjectDataOutput) WriteInt32Array(v []int32) {
	var length int32
	length = int32(len(v))
	o.WriteInt32(length)
	for j := int32(0); j < length; j++ {
		o.WriteInt32(v[j])
	}
}

func (o *ObjectDataOutput) WriteInt64Array(v []int64) {
	var length int32
	length = int32(len(v))
	o.WriteInt32(length)
	for j := int32(0); j < length; j++ {
		o.WriteInt64(v[j])
	}
}

func (o *ObjectDataOutput) WriteFloat32Array(v []float32) {
	var length int32
	length = int32(len(v))
	o.WriteInt32(length)
	for j := int32(0); j < length; j++ {
		o.WriteFloat32(v[j])
	}
}

func (o *ObjectDataOutput) WriteFloat64Array(v []float64) {
	var length int32
	length = int32(len(v))
	o.WriteInt32(length)
	for j := int32(0); j < length; j++ {
		o.WriteFloat64(v[j])
	}
}

//// ObjectDataInput ////

type ObjectDataInput struct {
	buffer    []byte
	offset    int
	service   *SerializationService
	bigIndian bool
	position  int
}

func NewObjectDataInput(buffer []byte, offset int, service *SerializationService, bigIndian bool) *ObjectDataInput {
	return &ObjectDataInput{buffer, offset, service, bigIndian, offset}
}

func (i *ObjectDataInput) AssertAvailable(numOfBytes int) error {
	if i.position < 0 || i.position+numOfBytes > len(i.buffer) {
		return errors.New("The remaining number of bytes is less than wanted number of bytes!")
	} else {
		return nil
	}
}

func (i *ObjectDataInput) ReadInt32() (int32, error) {
	var err error = i.AssertAvailable(INT_SIZE_IN_BYTES)
	var ret int32
	if err == nil {
		ret = ReadInt32(i.buffer, i.position, i.bigIndian)
		i.position += INT_SIZE_IN_BYTES
		return ret, err
	} else {
		return ret, err
	}
}

func (i *ObjectDataInput) ReadInt32WithPosition(pos int) (int32, error) {
	var err error = i.AssertAvailable(INT_SIZE_IN_BYTES)
	var ret int32
	if err == nil {
		ret = ReadInt32(i.buffer, pos, i.bigIndian)
		return ret, err
	} else {
		return ret, err
	}
}

func (i *ObjectDataInput) ReadFloat32() (float32, error) {
	var err error = i.AssertAvailable(FLOAT_SIZE_IN_BYTES)
	var ret float32
	if err == nil {
		ret = ReadFloat32(i.buffer, i.position, i.bigIndian)
		i.position += FLOAT_SIZE_IN_BYTES
		return ret, err
	} else {
		return ret, err
	}
}

func (i *ObjectDataInput) ReadFloat64() (float64, error) {
	var err error = i.AssertAvailable(DOUBLE_SIZE_IN_BYTES)
	var ret float64
	if err == nil {
		ret = ReadFloat64(i.buffer, i.position, i.bigIndian)
		i.position += DOUBLE_SIZE_IN_BYTES
		return ret, err
	} else {
		return ret, err
	}
}

func (i *ObjectDataInput) ReadFloat64WithPosition(pos int) (float64, error) {
	var err error = i.AssertAvailable(DOUBLE_SIZE_IN_BYTES)
	var ret float64
	if err == nil {
		ret = ReadFloat64(i.buffer, pos, i.bigIndian)
		return ret, err
	} else {
		return ret, err
	}
}

func (i *ObjectDataInput) ReadBool() (bool, error) {
	var err error = i.AssertAvailable(BOOL_SIZE_IN_BYTES)
	var ret bool
	if err == nil {
		ret = ReadBool(i.buffer, i.position)
		i.position += BOOL_SIZE_IN_BYTES
		return ret, err
	} else {
		return ret, err
	}
}

func (i *ObjectDataInput) ReadBoolWithPosition(pos int) (bool, error) {
	var err error = i.AssertAvailable(BOOL_SIZE_IN_BYTES)
	var ret bool
	if err == nil {
		ret = ReadBool(i.buffer, pos)
		return ret, err
	} else {
		return ret, err
	}
}

func (i *ObjectDataInput) ReadUTF() string {
	var length int32
	length, _ = i.ReadInt32()
	var ret []rune = make([]rune, length)
	for j := 0; j < int(length); j++ {
		r, n := utf8.DecodeRune(i.buffer[i.position:])
		i.position += n
		ret[j] = r
	}
	return string(ret)
}

func (i *ObjectDataInput) ReadObject() interface{} {
	return i.service.ReadObject(i)
}

func (i *ObjectDataInput) ReadByte() (byte, error) {
	var err error = i.AssertAvailable(BYTE_SIZE_IN_BYTES)
	var ret byte
	if err == nil {
		ret = ReadUInt8(i.buffer, i.position)
		i.position += BYTE_SIZE_IN_BYTES
		return ret, err
	} else {
		return ret, err
	}
}

func (i *ObjectDataInput) ReadInt16() (int16, error) {
	var err error = i.AssertAvailable(SHORT_SIZE_IN_BYTES)
	var ret int16
	if err == nil {
		ret = ReadInt16(i.buffer, i.position, i.bigIndian)
		i.position += SHORT_SIZE_IN_BYTES
		return ret, err
	} else {
		return ret, err
	}
}

func (i *ObjectDataInput) ReadInt64() (int64, error) {
	var err error = i.AssertAvailable(LONG_SIZE_IN_BYTES)
	var ret int64
	if err == nil {
		ret = ReadInt64(i.buffer, i.position, i.bigIndian)
		i.position += LONG_SIZE_IN_BYTES
		return ret, err
	} else {
		return ret, err
	}
}

func (i *ObjectDataInput) ReadInt16Array() []int16 {
	length, _ := i.ReadInt32()
	var arr []int16 = make([]int16, length)
	for j := int32(0); j < length; j++ {
		arr[j], _ = i.ReadInt16()
	}
	return arr
}

func (i *ObjectDataInput) ReadInt32Array() []int32 {
	length, _ := i.ReadInt32()
	var arr []int32 = make([]int32, length)
	for j := int32(0); j < length; j++ {
		arr[j], _ = i.ReadInt32()
	}
	return arr
}

func (i *ObjectDataInput) ReadInt64Array() []int64 {
	length, _ := i.ReadInt32()
	var arr []int64 = make([]int64, length)
	for j := int32(0); j < length; j++ {
		arr[j], _ = i.ReadInt64()
	}
	return arr
}

func (i *ObjectDataInput) ReadFloat32Array() []float32 {
	length, _ := i.ReadInt32()
	var arr []float32 = make([]float32, length)
	for j := int32(0); j < length; j++ {
		arr[j], _ = i.ReadFloat32()
	}
	return arr
}

func (i *ObjectDataInput) ReadFloat64Array() []float64 {
	length, _ := i.ReadInt32()
	var arr []float64 = make([]float64, length)
	for j := int32(0); j < length; j++ {
		arr[j], _ = i.ReadFloat64()
	}
	return arr
}
