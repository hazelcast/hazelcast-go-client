package serialization

import (
	"encoding/binary"
	"math"
	. "github.com/hazelcast/go-client/internal/common"
)

const (
	TYPE_OFFSET        = 4
	DATA_OFFSET        = 8
	HEAP_DATA_OVERHEAD = 8
)

type Data struct {
	Payload []byte
}

func (data Data) Buffer() []byte {
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
	return int(math.Max(float64(d.TotalSize()-HEAP_DATA_OVERHEAD), 0))
}

func (d *Data) GetPartitionHash() int32 {
	return Murmur3ADefault(d.Payload, DATA_OFFSET, d.DataSize())
}
