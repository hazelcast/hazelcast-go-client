package serialization

import (
	"testing"
	"fmt"
	."github.com/hazelcast/go-client/internal/serialization/api"
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
		return employee{}
	} else {
		return nil
	}
}

type employee struct {
	age  int32
	name string
}

func (e employee) ReadData(input DataInput) interface{} {
	e.age, _ = input.ReadInt32()
	e.name= input.ReadUTF()
	input.ReadInt32()
	return nil
}

func (e employee) WriteData(output DataOutput) {
	output.WriteInt32(e.age)
	output.WriteUTF(e.name)
}

func (employee) GetFactoryId() int32 {
	return 4
}

func (employee) GetClassId() int32 {
	return 1
}

func TestIdentifiedDataSerializableSerializer_Write(t *testing.T) {

	factories := make(map[int32]IdentifiedDataSerializableFactory)
	factories[4] = factory{}
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	o := NewObjectDataOutput(9, NewSerializationService(), false)
	in := NewObjectDataInput(buf, 0, NewSerializationService(), false)

	var employee1 employee = employee{22, "Furkan"}
	serializer := NewIdentifiedDataSerializableSerializer(factories)

	serializer.Write(o, employee1)
	serializer.Read(in)
	fmt.Println(in.buffer)

}
