package serialization

import (
	"errors"
)

type ObjectDataOutput struct {
	buffer    []byte
	service  serializationService
	bigIndian bool
	position  int
}

func NewObjectDataOutput(length int, service serializationService, bigIndian bool) *ObjectDataOutput {
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

func (o *ObjectDataOutput) WriteInt(v int32) {
	o.EnsureAvailable(INT_SIZE_IN_BYTES) // will be imported by common.go
	WriteInt(o.buffer, v, o.position, o.bigIndian)
	o.position += INT_SIZE_IN_BYTES
}

//// ObjectDataInput ////

type ObjectDataInput struct {
	buffer    []byte
	offset    int
	service   serializationService
	bigIndian bool
	position  int
}

func NewObjectDataInput(buffer []byte, offset int, service serializationService, bigIndian bool) *ObjectDataInput {
	return &ObjectDataInput{buffer, offset, service, bigIndian, offset}
}

func (o *ObjectDataInput) ReadInt(pos int) (int32, error) {
	var err error = o.AssertAvailable(INT_SIZE_IN_BYTES)
	var ret int32
	if err == nil {
		if pos == UNDEFINED_POSITION {
			ret = ReadInt(o.buffer, o.position, o.bigIndian)
			o.position += INT_SIZE_IN_BYTES
		} else {
			ret = ReadInt(o.buffer, pos, o.bigIndian)
		}
		return ret, err
	} else {
		return ret, err
	}
}

func (o ObjectDataInput) AssertAvailable(numOfBytes int) error {
	if o.position < 0 || o.position+numOfBytes > len(o.buffer) {
		return errors.New("The remaining number of bytes is less than wanted number of bytes!")
	} else {
		return nil
	}
}
