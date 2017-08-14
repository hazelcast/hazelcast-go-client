package serialization

import (
	"testing"
)


func TestSerializationService_LookUpDefaultSerializer(t *testing.T) {
	var a int32 = 5
	var id int32 = NewSerializationService().LookUpDefaultSerializer(a).GetId()
	var expectedId int32 = -7
	if id != expectedId {
		t.Errorf("LookUpDefaultSerializer() returns ", id, " expected ", expectedId)
	}
}

func TestSerializationService_ToData(t *testing.T) {
	var expected int32 = 5
	service := NewSerializationService();
	data,_ := service.ToData(expected)
	var ret int32
	temp,_:= service.ToObject(data)
	ret=temp.(int32)
	if expected != ret {
		t.Errorf("ToData() returns ", ret, " expected ", expected)
	}

}

