package serialization

import (
	"github.com/hazelcast/go-client/config"
	. "github.com/hazelcast/go-client/internal/serialization/api"
	"testing"
	"log"
	. "github.com/hazelcast/go-client/internal/serialization/api"
	"testing"
)

func TestInteger32Serializer_Write(t *testing.T) {
	i := Integer32Serializer{}
	o := NewObjectDataOutput(9, &SerializationService{}, false)
	var a int32 = 5
	var expectedRet int32 = 7
	i.Write(o, a)
	i.Write(o, expectedRet)
	in := NewObjectDataInput(o.buffer, 4, &SerializationService{}, false)
	ret := i.Read(in)

	if ret != expectedRet {
		t.Errorf("ToData() returns ", ret, " expected ", expectedRet)
	}
}

type factory struct{}

func (factory) Create(classId int32) IdentifiedDataSerializable {
	if classId == 1 {
		return &employee{}
	} else {
		return nil
	}
}

type employee struct {
	age  int32
	name string
}

func (e *employee) ReadData(input DataInput) {
	e.age, _ = input.ReadInt32()
	e.name = input.ReadUTF()
}

func (e *employee) WriteData(output DataOutput) {
	output.WriteInt32(e.age)
	output.WriteUTF(e.name)
}

func (*employee) GetFactoryId() int32 {
	return 4
}

func (*employee) GetClassId() int32 {
	return 1
}

func x(i interface{}) {
	y(i)
}

func y(i interface{}) {
	_, ok := i.(IdentifiedDataSerializable)
	log.Println(ok)
}

func TestIdentifiedDataSerializableSerializer_Write(t *testing.T) {
	var employee1 employee = employee{22, "Furkan"}
	c := config.NewSerializationConfig()
	c.AddDataSerializableFactory(factory{}, employee1.GetFactoryId())

	service := NewSerializationService(c)

	data, _ := service.ToData(&employee1)
	ret_employee, _ := service.ToObject(data)

	if employee1 != *ret_employee.(*employee) {
		t.Errorf("IdentifiedDataSerializable() works wrong!")
	}

}
