package serialization

import (
	"encoding/binary"
	."github.com/hazelcast/go-client/internal/common"
)

const (
	TYPE_OFFSET = 4
	DATA_OFFSET = 8
)

type Data struct {
	Payload []byte
}

func (data Data) Buffer() []byte{
	return data.Payload
}

func (data Data) getType() int32 {
	if data.TotalSize() == 0 {
		return 0
	}
	return int32(binary.BigEndian.Uint32(data.Payload[TYPE_OFFSET:]))
}

func (data Data) TotalSize() int {
	if data.Payload == nil {
		return 0
	}
	return len(data.Payload)
}

func (d *Data) DataSize() int {
	return len(d.Payload) + INT_SIZE_IN_BYTES
}

func (d *Data) GetPartitionHash() int32 {
	//TODO :: Remove the second return
	//return Murmur3ADefault(d.Payload, DATA_OFFSET, d.DataSize())
	return Murmur3ADefault(d.Payload, 0, d.DataSize()-4)

}

type dataOutput interface {
	WriteInt32(v int32)
	WriteFloat64(v float64)
}

type dataInput interface {
	ReadInt32() (int32, error)
	ReadInt32WithPosition(position int) (int32, error)
	ReadFloat64() (float64, error)
	ReadFloat64WithPosition(position int) (float64, error)
}
