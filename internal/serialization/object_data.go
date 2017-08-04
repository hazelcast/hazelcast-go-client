package serialization

import (
	"errors"
	. "github.com/hazelcast/go-client/internal/common"
)

type ObjectDataOutput struct {
	buffer    []byte
	service  SerializationService
	bigIndian bool
	position  int
}

func NewObjectDataOutput(length int, service SerializationService, bigIndian bool) *ObjectDataOutput {
	return &ObjectDataOutput{make([]byte, length), service, bigIndian, 0}
}

func (o ObjectDataOutput) Available() int {
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
	o.EnsureAvailable(INT_SIZE_IN_BYTES) // will be imported by common.go
	WriteInt32(o.buffer, v, o.position, o.bigIndian)
	o.position += INT_SIZE_IN_BYTES
}

func (o *ObjectDataOutput) WriteFloat64(v float64) {
	o.EnsureAvailable(DOUBLE_SIZE_IN_BYTES) // will be imported by common.go
	WriteFloat64(o.buffer, v, o.position, o.bigIndian)
	o.position += DOUBLE_SIZE_IN_BYTES
}

//// ObjectDataInput ////

type ObjectDataInput struct {
	buffer    []byte
	offset    int
	service   SerializationService
	bigIndian bool
	position  int
}

func NewObjectDataInput(buffer []byte, offset int, service SerializationService, bigIndian bool) *ObjectDataInput {
	return &ObjectDataInput{buffer, offset, service, bigIndian, offset}
}

func (o ObjectDataInput) AssertAvailable(numOfBytes int) error {
	if o.position < 0 || o.position+numOfBytes > len(o.buffer) {
		return errors.New("The remaining number of bytes is less than wanted number of bytes!")
	} else {
		return nil
	}
}

func (o *ObjectDataInput) ReadInt32() (int32, error) {
	var err error = o.AssertAvailable(INT_SIZE_IN_BYTES)
	var ret int32
	if err == nil {
		ret = ReadInt32(o.buffer, o.position, o.bigIndian)
		o.position += INT_SIZE_IN_BYTES
		return ret, err
	} else {
		return ret, err
	}
}

func (o *ObjectDataInput) ReadInt32WithPosition(pos int) (int32, error) {
	var err error = o.AssertAvailable(INT_SIZE_IN_BYTES)
	var ret int32
	if err == nil {
		ret = ReadInt32(o.buffer, pos, o.bigIndian)
		return ret, err
	} else {
		return ret, err
	}
}

func (o *ObjectDataInput) ReadFloat64() (float64, error) {
	var err error = o.AssertAvailable(DOUBLE_SIZE_IN_BYTES)
	var ret float64
	if err == nil {
		ret = ReadFloat64(o.buffer, o.position, o.bigIndian)
		o.position += DOUBLE_SIZE_IN_BYTES
		return ret, err
	} else {
		return ret, err
	}
}

func (o *ObjectDataInput) ReadFloat64WithPosition(pos int) (float64, error) {
	var err error = o.AssertAvailable(DOUBLE_SIZE_IN_BYTES)
	var ret float64
	if err == nil {
		ret = ReadFloat64(o.buffer, pos, o.bigIndian)
		return ret, err
	} else {
		return ret, err
	}
}

