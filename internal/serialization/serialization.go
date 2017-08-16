package serialization

import (
	"reflect"
	"errors"
	. "github.com/hazelcast/go-client/config"
	. "github.com/hazelcast/go-client/internal/serialization/api"
)

type Serializer interface {
	GetId() int32
	Read(input DataInput) interface{}
	Write(output DataOutput, object interface{})
}

////// SerializationService ///////////
type SerializationService struct {
	serializationConfig *SerializationConfig
	registry            map[int32]Serializer // key=id of serializer, serializer will be a class=>> default serializer + custom +global
	nameToId            map[string]int32
}

func NewSerializationService(serializationConfig *SerializationConfig) *SerializationService {
	v1 := SerializationService{serializationConfig: serializationConfig, nameToId: make(map[string]int32), registry: make(map[int32]Serializer)}
	v1.RegisterDefaultSerializers()
	return &v1
}

func (service *SerializationService) ToData(object interface{}) (*Data, error) {
	//TODO should return proper error values
	dataOutput := NewObjectDataOutput(1, service, service.serializationConfig.IsBigEndian)
	var serializer Serializer
	serializer = service.FindSerializerFor(object)
	dataOutput.WriteInt32(0) // partition
	dataOutput.WriteInt32(serializer.GetId())
	serializer.Write(dataOutput, object)
	return &Data{dataOutput.buffer}, nil
}

func (service *SerializationService) ToObject(data *Data) (interface{}, error) {
	//TODO should return proper error values
	if data == nil {
		return data, nil
	}
	if data.getType() == 0 {
		return data, nil
	}
	var serializer = service.registry[data.getType()]
	dataInput := NewObjectDataInput(data.Payload, DATA_OFFSET, service, service.serializationConfig.IsBigEndian)
	return serializer.Read(dataInput), nil
}

func (service *SerializationService) WriteObject(output *ObjectDataOutput, object interface{}) {
	var serializer = service.FindSerializerFor(object)
	output.WriteInt32(serializer.GetId())
	serializer.Write(output, object)
}

func (service *SerializationService) ReadObject(input *ObjectDataInput) interface{} {
	var serializerId, _ = input.ReadInt32()
	var serializer = service.registry[serializerId]
	return serializer.Read(input)
}

func (service *SerializationService) FindSerializerFor(obj interface{}) Serializer {
	var serializer Serializer
	if obj == nil {
		serializer = service.registry[service.nameToId["nil"]]
	}

	if serializer == nil {
		serializer = service.LookUpDefaultSerializer(obj)
	}

	if serializer == nil {
		//serializer=lookupCustomSerializer(obj)
	}

	if serializer == nil {
		//serializer=lookupGlobalSerializer
	}

	if serializer == nil {
		//serializer=findSerializerByName('!json', false)
	}

	if serializer == nil {
		// throw "There is no suitable serializer for " +obj + "."
	}
	return serializer
}

func (service *SerializationService) RegisterDefaultSerializers() {
	service.RegisterSerializer(&ByteSerializer{})
	service.nameToId["uint8"] = (&ByteSerializer{}).GetId()

	service.RegisterSerializer(&BoolSerializer{})
	service.nameToId["bool"] = (&BoolSerializer{}).GetId()

	service.RegisterSerializer(&Integer16Serializer{})
	service.nameToId["int16"] = (&Integer16Serializer{}).GetId()

	service.RegisterSerializer(&Integer32Serializer{})
	service.nameToId["int32"] = (&Integer32Serializer{}).GetId()

	service.RegisterSerializer(&Integer64Serializer{})
	service.nameToId["int64"] = (&Integer64Serializer{}).GetId()

	service.RegisterSerializer(&Float32Serializer{})
	service.nameToId["float32"] = (&Float32Serializer{}).GetId()

	service.RegisterSerializer(&Float64Serializer{})
	service.nameToId["float64"] = (&Float64Serializer{}).GetId()

	service.RegisterSerializer(&StringSerializer{})
	service.nameToId["string"] = (&StringSerializer{}).GetId()

	service.RegisterSerializer(&NilSerializer{})
	service.nameToId["nil"] = (&NilSerializer{}).GetId()

	service.RegisterSerializer(&Integer16ArraySerializer{})
	service.nameToId["[]int16"] = (&Integer16ArraySerializer{}).GetId()

	service.RegisterSerializer(&Integer32ArraySerializer{})
	service.nameToId["[]int32"] = (&Integer32ArraySerializer{}).GetId()

	service.RegisterSerializer(&Integer64ArraySerializer{})
	service.nameToId["[]int64"] = (&Integer64ArraySerializer{}).GetId()

	service.RegisterSerializer(&Float32ArraySerializer{})
	service.nameToId["[]float32"] = (&Float32ArraySerializer{}).GetId()

	service.RegisterSerializer(&Float64ArraySerializer{})
	service.nameToId["[]float64"] = (&Float64ArraySerializer{}).GetId()

	service.RegisterIdentifiedFactories()
}

func (service *SerializationService) RegisterSerializer(serializer Serializer) error {
	if service.registry[serializer.GetId()] != nil {
		return errors.New("This serializer is already in the registry!")
	}
	service.registry[serializer.GetId()] = serializer
	return nil
}

func (service *SerializationService) GetIdByObject(obj interface{}) int32 {
	return service.nameToId[reflect.TypeOf(obj).String()]
}

func (service *SerializationService) LookUpDefaultSerializer(obj interface{}) Serializer {
	var serializer Serializer
	if isIdentifiedDataSerializable(obj) {
		return service.registry[service.nameToId["identified"]]
	}

	// if isPortableSerializer
	///	objectType:=reflect.TypeOf(obj).String()
	serializer = service.registry[service.GetIdByObject(obj)]

	return serializer
}

func (service *SerializationService) RegisterIdentifiedFactories() {
	factories := make(map[int32]IdentifiedDataSerializableFactory)
	for id, _ := range service.serializationConfig.DataSerializableFactories {
		factories[id] = service.serializationConfig.DataSerializableFactories[id]
	}

	idToPredicate := make(map[int32]IdentifiedDataSerializable)
	FillPredicateIds(idToPredicate)
	factories[PREDICATE_FACTORY_ID] = &PredicateFactory{idToPredicate}

	//factories[RELIABLE_TOPIC_MESSAGE_FACTORY_ID] = new ReliableTopicMessageFactory()
	//factories[CLUSTER_DATA_FACTORY_ID] = new ClusterDataFactory()
	service.RegisterSerializer(&IdentifiedDataSerializableSerializer{factories})
	service.nameToId["identified"] = (&IdentifiedDataSerializableSerializer{}).GetId()
}

func FillPredicateIds(idToPredicate map[int32]IdentifiedDataSerializable) {
	idToPredicate[0] = &SqlPredicate{}
}

func isIdentifiedDataSerializable(obj interface{}) bool {
	_, ok := obj.(IdentifiedDataSerializable)
	if ok {
		return true
	}
	return false
}
