package serialization

import (
	"bytes"
	"encoding/gob"
	. "github.com/hazelcast/go-client/config"
	"github.com/hazelcast/go-client/internal/serialization/api"
	"log"
	"reflect"
	"testing"
)

func TestSerializationService_LookUpDefaultSerializer(t *testing.T) {
	var a int32 = 5
	var id int32 = NewSerializationService(NewSerializationConfig()).lookUpDefaultSerializer(a).Id()
	var expectedId int32 = -7
	if id != expectedId {
		t.Errorf("LookUpDefaultSerializer() returns ", id, " expected ", expectedId)
	}
}

func TestSerializationService_ToData(t *testing.T) {
	var expected int32 = 5
	c := NewSerializationConfig()
	service := NewSerializationService(c)
	data, _ := service.ToData(expected)
	var ret int32
	temp, _ := service.ToObject(data)
	ret = temp.(int32)
	if expected != ret {
		t.Errorf("ToData() returns ", ret, " expected ", expected)
	}
}

type CustomMusicianSerializer struct {
}

func (s *CustomMusicianSerializer) Id() int32 {
	return 10
}

func (s *CustomMusicianSerializer) Read(input api.DataInput) (interface{}, error) {
	var network bytes.Buffer
	data, _ := input.ReadData()
	network.Write(data.Buffer())
	dec := gob.NewDecoder(&network)
	var v musician
	dec.Decode(&v)
	return &v, nil
}

func (s *CustomMusicianSerializer) Write(output api.DataOutput, obj interface{}) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(obj)
	if err != nil {
		log.Fatal("encode:", err)
	}
	payload := (&network).Bytes()
	output.WriteData(NewData(payload))

}

type musician struct {
	Name    string
	Surname string
}

func TestCustomSerializer(t *testing.T) {
	m := &musician{"Furkan", "Şenharputlu"}
	customSerializer := &CustomMusicianSerializer{}
	config := NewSerializationConfig()
	config.AddCustomSerializer(reflect.TypeOf(m), customSerializer)
	service := NewSerializationService(config)
	data, _ := service.ToData(m)
	ret, _ := service.ToObject(data)

	if !reflect.DeepEqual(m, ret) {
		t.Errorf("custom serialization failed")
	}
}
