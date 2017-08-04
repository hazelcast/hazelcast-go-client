package serialization

import (
	"reflect"
	"errors"
	. "github.com/hazelcast/go-client/config"
)

type serializer interface {
	GetId() int32
	Read(input dataInput) interface{}
	Write(output dataOutput, object interface{})
}

////// SerializationService ///////////
type SerializationService struct {
	serializationConfig SerializationConfig
	registry            map[int32]serializer // key=id of serializer, serializer will be a class=>> default serializer + custom +global
	nameToId            map[string]int32
}

func NewSerializationService() SerializationService {
	v1 := SerializationService{serializationConfig: NewSerializationConfig(), nameToId: make(map[string]int32), registry: make(map[int32]serializer)}
	v1.RegisterDeafultSerializers()
	return v1
}

func (service SerializationService) ToData(object interface{}) (*Data, error) {
	//TODO should return proper error values
	dataOutput := NewObjectDataOutput(1, service, service.serializationConfig.IsBigEndian)
	var serializer serializer
	serializer = service.FindSerializerFor(object)
	dataOutput.WriteInt32(0) // partition
	dataOutput.WriteInt32(serializer.GetId())
	serializer.Write(dataOutput, object)
	return &Data{dataOutput.buffer}, nil
}

func (service SerializationService) ToObject(data *Data) (interface{}, error) {
	//TODO should return proper error values
	if data == nil {
		return data,nil
	}
	if data.getType() == 0 {
		return data, nil
	}
	var serializer = service.registry[data.getType()]
	dataInput := NewObjectDataInput(data.Payload, DATA_OFFSET, service, service.serializationConfig.IsBigEndian)
	return serializer.Read(dataInput), nil
}

func (service SerializationService) WriteObject(output dataOutput, object interface{}) {
	var serializer = service.FindSerializerFor(object)
	output.WriteInt32(serializer.GetId())
	serializer.Write(output, object)
}

func (service SerializationService) ReadObject(input dataInput) interface{} {
	var serializerId, _ = input.ReadInt32()
	var serializer = service.registry[serializerId]
	return serializer.Read(input)
}

func (service SerializationService) FindSerializerFor(obj interface{}) serializer {
	var serializer serializer
	if obj == nil {
		//serializer = findSerializerByName('null', false);
	}

	if (serializer == nil) {
		serializer = service.LookUpDefaultSerializer(obj)
	}

	if (serializer == nil) {
		//serializer=lookupCustomSerializer(obj)
	}

	if (serializer == nil) {
		//serializer=lookupGlobalSerializer
	}

	if (serializer == nil) {
		//serializer=lookupDefaultSerializer
	}
	return serializer
}

func (service SerializationService) RegisterDeafultSerializers() {
	service.RegisterSerializer(IntegerSerializer{})
	service.nameToId["int32"] = IntegerSerializer{}.GetId()
}

func (service SerializationService) RegisterSerializer(serializer serializer) error {
	if service.registry[serializer.GetId()] != nil {
		return errors.New("This serializer is already in the registry!")
	}
	service.registry[serializer.GetId()] = serializer
	return nil
}

func (service SerializationService) GetIdByObject(obj interface{}) int32 {
	return service.nameToId[reflect.TypeOf(obj).String()]
}

func (service SerializationService) LookUpDefaultSerializer(obj interface{}) serializer {
	var serializer serializer
	//  if isIdentifiedDataSerializable(obj)
	// if isPortableSerializer
	///	objectType:=reflect.TypeOf(obj).String()
	serializer = service.registry[service.GetIdByObject(obj)]

	return serializer
}

/*func isIdentifiedDataSerializable(obj *interface{}) bool {
	return nil//( obj.readData && obj.writeData && obj.getClassId && obj.getFactoryId);
}*/
