package hazelcast

import (
	"encoding/gob"
	"bytes"
	"log"
)

const (
	DATA_PARTITION_HASH_OFFSET = 0
	DATA_TYPE_OFFSET = 4
	DATA_OFFSET = 8
	HEAP_DATA_OVERHEAD = DATA_OFFSET
)

type SerializationService struct {

}

type Data struct {
	buffer []byte
}
func (d *Data) CalculateSize() int{
	return len(d.buffer) + INT_SIZE_IN_BYTES
}
func (ss *SerializationService) ToData(obj *interface{}) (*Data, error)  {
	var w bytes.Buffer
	enc:=gob.NewEncoder(&w)
	err:=enc.Encode(obj)
	if err != nil {
		log.Fatal("Encode:", err)
		return nil, err
	}
	return &Data{}, nil
}

func (ss *SerializationService) ToObject(data *Data) (*interface{}, error) {
	return nil,nil
}