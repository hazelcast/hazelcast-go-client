package serialization

import "encoding/binary"

const (
	TYPE_OFFSET = 4
	DATA_OFFSET = 8
)

type data struct {
	payload []byte
}

func (data data) getType() int32 {
	if data.totalSize() == 0 {
		return 0
	}
	return int32(binary.BigEndian.Uint32(data.payload[TYPE_OFFSET:]))
}

func (data data) totalSize() int {
	if data.payload == nil {
		return 0
	}
	return len(data.payload)
}

type dataOutput interface {
	WriteInt(number int32)
}

type dataInput interface {
	ReadInt(position int) (int32, error)
}
