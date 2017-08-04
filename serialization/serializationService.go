package serialization

import (
	"reflect"
	"errors"
	"go-client"
)

type serializer interface {
	GetId() int32
	Read(input dataInput) interface{}
	Write(output dataOutput, object interface{})
}

////// SerializationService ///////////
type serializationService struct {
	serializationConfig hazelcast.SerializationConfig
	registry            map[int32]serializer // key=id of serializer, serializer will be a class=>> default serializer + custom +global
	nameToId            map[string]int32
}

func NewSerializationService() serializationService {
	v1 := serializationService{serializationConfig: hazelcast.NewSerializationConfig(), nameToId: make(map[string]int32), registry: make(map[int32]serializer)}
	v1.RegisterDeafultSerializers()
	return v1
}

func (service serializationService) ToData(object interface{}) data {
	dataOutput := NewObjectDataOutput(1, service, service.serializationConfig.IsBigEndian)
	var serializer serializer
	serializer = service.FindSerializerFor(object)
	dataOutput.WriteInt(0) // partition
	dataOutput.WriteInt(serializer.GetId())
	serializer.Write(dataOutput, object)
	return data{dataOutput.buffer}
}

func (service serializationService) ToObject(data *data) interface{} {
	if data == nil {
		return data
	}
	if data.getType() == 0 {
		return data
	}
	var serializer = service.registry[data.getType()]
	dataInput := NewObjectDataInput(data.payload, DATA_OFFSET, service, service.serializationConfig.IsBigEndian)
	return serializer.Read(dataInput)
}

func (service serializationService) WriteObject(output dataOutput, object interface{}) {
	var serializer = service.FindSerializerFor(object)
	output.WriteInt(serializer.GetId())
	serializer.Write(output, object)
}

func (service serializationService) ReadObject(input dataInput) interface{} {
	var serializerId, _ = input.ReadInt(UNDEFINED_POSITION)
	var serializer = service.registry[serializerId]
	return serializer.Read(input)
}

func (service serializationService) FindSerializerFor(obj interface{}) serializer {
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

func (service serializationService) RegisterDeafultSerializers() {
	service.RegisterSerializer(IntegerSerializer{})
	service.nameToId["int32"] = IntegerSerializer{}.GetId()
}

func (service serializationService) RegisterSerializer(serializer serializer) error {
	if service.registry[serializer.GetId()] != nil {
		return errors.New("This serializer is already in the registry!")
	}
	service.registry[serializer.GetId()] = serializer
	return nil
}

func (service serializationService) GetIdByObject(obj interface{}) int32 {
	return service.nameToId[reflect.TypeOf(obj).String()]
}

func (service serializationService) LookUpDefaultSerializer(obj interface{}) serializer {
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
