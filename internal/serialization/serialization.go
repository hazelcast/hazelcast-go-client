package serialization

import (
	"bytes"
	"encoding/gob"
	"log"

	. "github.com/hazelcast/go-client/internal"
)

const (
	DATA_PARTITION_HASH_OFFSET = 0
	DATA_TYPE_OFFSET           = 4
	DATA_OFFSET                = 8
	HEAP_DATA_OVERHEAD         = DATA_OFFSET
)

type SerializationService struct {
}

type Data struct {
	Buffer []byte
}

func (d *Data) TotalSize() int {
	if d.Buffer != nil {
		return len(d.Buffer)
	}
	return 0
}
func (d *Data) DataSize() int {
	return len(d.Buffer) + INT_SIZE_IN_BYTES
}

func (d *Data) GetPartitionHash() int32 {
	return Murmur3ADefault(d.Buffer, DATA_OFFSET, d.DataSize())
}

func (ss *SerializationService) ToData(obj interface{}) (*Data, error) {
	var w bytes.Buffer
	enc := gob.NewEncoder(&w)
	err := enc.Encode(obj)
	if err != nil {
		log.Fatal("Encode:", err)
		return nil, err
	}
	return &Data{}, nil
}

func (ss *SerializationService) ToObject(data *Data) (interface{}, error) {
	return nil, nil
}
