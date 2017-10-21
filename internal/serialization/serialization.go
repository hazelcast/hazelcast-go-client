package serialization

import (
	"errors"
	. "github.com/hazelcast/go-client/config"
	. "github.com/hazelcast/go-client/internal/serialization/api"
	"reflect"
)

type Serializer interface {
	Id() int32
	Read(input DataInput) (interface{}, error)
	Write(output DataOutput, object interface{})
}

////// SerializationService ///////////
type SerializationService struct {
	serializationConfig *SerializationConfig
	registry            map[int32]Serializer
	nameToId            map[string]int32
}

func NewSerializationService(serializationConfig *SerializationConfig) *SerializationService {
	v1 := SerializationService{serializationConfig: serializationConfig, nameToId: make(map[string]int32), registry: make(map[int32]Serializer)}
	v1.RegisterDefaultSerializers()
	return &v1
}

func (service *SerializationService) ToData(object interface{}) (*Data, error) {
	//TODO should return proper error values
	dataOutput := NewPositionalObjectDataOutput(1, service, service.serializationConfig.IsBigEndian())
	serializer := service.FindSerializerFor(object)
	dataOutput.WriteInt32(0) // partition
	dataOutput.WriteInt32(serializer.Id())
	serializer.Write(dataOutput, object)
	return &Data{dataOutput.buffer}, nil
}

func (service *SerializationService) ToObject(data *Data) (interface{}, error) {
	//TODO should return proper error values
	if data == nil {
		return nil, nil
	}
	if data.getType() == 0 {
		return data, nil
	}
	var serializer = service.registry[data.getType()]
	dataInput := NewObjectDataInput(data.Payload, DATA_OFFSET, service, service.serializationConfig.IsBigEndian())
	return serializer.Read(dataInput)
}

func (service *SerializationService) WriteObject(output DataOutput, object interface{}) {
	var serializer = service.FindSerializerFor(object)
	output.WriteInt32(serializer.Id())
	serializer.Write(output, object)
}

func (service *SerializationService) ReadObject(input DataInput) (interface{}, error) {
	serializerId, err := input.ReadInt32()
	if err != nil {
		return nil, err
	}
	serializer := service.registry[serializerId]
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
	service.nameToId["uint8"] = CONSTANT_TYPE_BYTE

	service.RegisterSerializer(&BoolSerializer{})
	service.nameToId["bool"] = CONSTANT_TYPE_BOOLEAN

	service.RegisterSerializer(&UInteger16Serializer{})
	service.nameToId["uint16"] = CONSTANT_TYPE_CHAR

	service.RegisterSerializer(&Integer16Serializer{})
	service.nameToId["int16"] = CONSTANT_TYPE_SHORT

	service.RegisterSerializer(&Integer32Serializer{})
	service.nameToId["int32"] = CONSTANT_TYPE_INTEGER

	service.RegisterSerializer(&Integer64Serializer{})
	service.nameToId["int64"] = CONSTANT_TYPE_LONG

	service.RegisterSerializer(&Float32Serializer{})
	service.nameToId["float32"] = CONSTANT_TYPE_FLOAT

	service.RegisterSerializer(&Float64Serializer{})
	service.nameToId["float64"] = CONSTANT_TYPE_DOUBLE

	service.RegisterSerializer(&StringSerializer{})
	service.nameToId["string"] = CONSTANT_TYPE_STRING

	service.RegisterSerializer(&NilSerializer{})
	service.nameToId["nil"] = CONSTANT_TYPE_NULL

	service.RegisterSerializer(&ByteArraySerializer{})
	service.nameToId["[]uint8"] = CONSTANT_TYPE_BYTE_ARRAY

	service.RegisterSerializer(&BoolArraySerializer{})
	service.nameToId["[]bool"] = CONSTANT_TYPE_BOOLEAN_ARRAY

	service.RegisterSerializer(&UInteger16ArraySerializer{})
	service.nameToId["[]uint16"] = CONSTANT_TYPE_CHAR_ARRAY

	service.RegisterSerializer(&Integer16ArraySerializer{})
	service.nameToId["[]int16"] = CONSTANT_TYPE_SHORT_ARRAY

	service.RegisterSerializer(&Integer32ArraySerializer{})
	service.nameToId["[]int32"] = CONSTANT_TYPE_INTEGER_ARRAY

	service.RegisterSerializer(&Integer64ArraySerializer{})
	service.nameToId["[]int64"] = CONSTANT_TYPE_LONG_ARRAY

	service.RegisterSerializer(&Float32ArraySerializer{})
	service.nameToId["[]float32"] = CONSTANT_TYPE_FLOAT_ARRAY

	service.RegisterSerializer(&Float64ArraySerializer{})
	service.nameToId["[]float64"] = CONSTANT_TYPE_DOUBLE_ARRAY

	service.RegisterSerializer(&StringArraySerializer{})
	service.nameToId["[]string"] = CONSTANT_TYPE_STRING_ARRAY

	service.RegisterIdentifiedFactories()

	service.RegisterSerializer(NewPortableSerializer(service, service.serializationConfig.PortableFactories(), service.serializationConfig.PortableVersion()))
	service.nameToId["!portable"] = CONSTANT_TYPE_PORTABLE
}

func (service *SerializationService) RegisterSerializer(serializer Serializer) error {
	if service.registry[serializer.Id()] != nil {
		return errors.New("This serializer is already in the registry!")
	}
	service.registry[serializer.Id()] = serializer
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
	if isPortableSerializable(obj) {
		return service.registry[service.nameToId["!portable"]]
	}
	serializer = service.registry[service.GetIdByObject(obj)]

	return serializer
}

func (service *SerializationService) RegisterIdentifiedFactories() {
	factories := make(map[int32]IdentifiedDataSerializableFactory)
	for id, _ := range service.serializationConfig.DataSerializableFactories() {
		factories[id] = service.serializationConfig.DataSerializableFactories()[id]
	}

	idToPredicate := make(map[int32]IdentifiedDataSerializable)
	FillPredicateIds(idToPredicate)
	factories[PREDICATE_FACTORY_ID] = &PredicateFactory{idToPredicate}

	//factories[RELIABLE_TOPIC_MESSAGE_FACTORY_ID] = new ReliableTopicMessageFactory()
	//factories[CLUSTER_DATA_FACTORY_ID] = new ClusterDataFactory()
	service.RegisterSerializer(NewIdentifiedDataSerializableSerializer(factories))
	service.nameToId["identified"] = CONSTANT_TYPE_DATA_SERIALIZABLE
}

func FillPredicateIds(idToPredicate map[int32]IdentifiedDataSerializable) {
	idToPredicate[0] = &SqlPredicate{}
}

func isIdentifiedDataSerializable(obj interface{}) bool {
	_, ok := obj.(IdentifiedDataSerializable)
	return ok
}

func isPortableSerializable(obj interface{}) bool {
	_, ok := obj.(Portable)
	return ok
}
