package serialization

import (
	"bytes"
	"encoding/gob"
	. "github.com/hazelcast/go-client/config"
	. "github.com/hazelcast/go-client/internal/serialization/api"
	"log"
	"reflect"
	"testing"
)

const (
	MUSICIAN_TYPE = 1
	PAINTER_TYPE  = 2
)

func TestSerializationService_LookUpDefaultSerializer(t *testing.T) {
	var a int32 = 5
	config := NewSerializationConfig()
	service := NewSerializationService(config, make(map[int32]IdentifiedDataSerializableFactory))
	var id int32 = service.lookUpDefaultSerializer(a).Id()
	var expectedId int32 = -7
	if id != expectedId {
		t.Errorf("LookUpDefaultSerializer() returns ", id, " expected ", expectedId)
	}
}

func TestSerializationService_ToData(t *testing.T) {
	var expected int32 = 5
	config := NewSerializationConfig()
	service := NewSerializationService(config, make(map[int32]IdentifiedDataSerializableFactory))
	data, _ := service.ToData(expected)
	var ret int32
	temp, _ := service.ToObject(data)
	ret = temp.(int32)
	if expected != ret {
		t.Errorf("ToData() returns ", ret, " expected ", expected)
	}
}

type CustomArtistSerializer struct {
}

func (s *CustomArtistSerializer) Id() int32 {
	return 10
}

func (s *CustomArtistSerializer) Read(input DataInput) (interface{}, error) {
	var network bytes.Buffer
	typ, _ := input.ReadInt32()
	data, _ := input.ReadData()
	network.Write(data.Buffer())
	dec := gob.NewDecoder(&network)
	var v artist
	if typ == MUSICIAN_TYPE {
		v = &musician{}
	} else if typ == PAINTER_TYPE {
		v = &painter{}
	}

	dec.Decode(v)
	return v, nil
}

func (s *CustomArtistSerializer) Write(output DataOutput, obj interface{}) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(obj)
	if err != nil {
		log.Fatal("encode:", err)
	}
	payload := (&network).Bytes()
	output.WriteInt32(obj.(artist).Type())
	output.WriteData(NewData(payload))

}

type artist interface {
	Type() int32
}

type musician struct {
	Name    string
	Surname string
}

func (*musician) Type() int32 {
	return MUSICIAN_TYPE
}

type painter struct {
	Name    string
	Surname string
}

func (*painter) Type() int32 {
	return PAINTER_TYPE
}

func TestCustomSerializer(t *testing.T) {
	m := &musician{"Furkan", "Şenharputlu"}
	p := &painter{"Leonardo", "da Vinci"}
	customSerializer := &CustomArtistSerializer{}
	config := NewSerializationConfig()

	config.AddCustomSerializer(reflect.TypeOf((*artist)(nil)).Elem(), customSerializer)
	service := NewSerializationService(config, make(map[int32]IdentifiedDataSerializableFactory))
	data, _ := service.ToData(m)
	ret, _ := service.ToObject(data)
	data2, _ := service.ToData(p)
	ret2, _ := service.ToObject(data2)

	if !reflect.DeepEqual(m, ret) || !reflect.DeepEqual(p, ret2) {
		t.Errorf("custom serialization failed")
	}
}
