package serialization

import (
	"fmt"
	. "github.com/hazelcast/go-client/config"
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/predicates"
	. "github.com/hazelcast/go-client/serialization"
	"reflect"
)

////// SerializationService ///////////
type SerializationService struct {
	serializationConfig *SerializationConfig
	registry            map[int32]Serializer
	nameToId            map[string]int32
}

func NewSerializationService(serializationConfig *SerializationConfig) *SerializationService {
	v1 := SerializationService{serializationConfig: serializationConfig, nameToId: make(map[string]int32), registry: make(map[int32]Serializer)}
	v1.registerDefaultSerializers()
	v1.registerCustomSerializers(serializationConfig.CustomSerializers())
	v1.registerGlobalSerializer(serializationConfig.GlobalSerializer())
	return &v1
}

func (service *SerializationService) ToData(object interface{}) (*Data, error) {
	dataOutput := NewPositionalObjectDataOutput(1, service, service.serializationConfig.IsBigEndian())
	serializer, err := service.FindSerializerFor(object)
	if err != nil {
		return nil, err
	}
	dataOutput.WriteInt32(0) // partition
	dataOutput.WriteInt32(serializer.Id())
	serializer.Write(dataOutput, object)
	return &Data{dataOutput.buffer}, nil
}

func (service *SerializationService) ToObject(data *Data) (interface{}, error) {
	if data == nil {
		return nil, nil
	}
	if data.GetType() == 0 {
		return data, nil
	}
	var serializer = service.registry[data.GetType()]
	dataInput := NewObjectDataInput(data.Buffer(), DATA_OFFSET, service, service.serializationConfig.IsBigEndian())
	return serializer.Read(dataInput)
}

func (service *SerializationService) WriteObject(output DataOutput, object interface{}) error {
	var serializer, err = service.FindSerializerFor(object)
	if err != nil {
		return err
	}
	output.WriteInt32(serializer.Id())
	serializer.Write(output, object)
	return nil
}

func (service *SerializationService) ReadObject(input DataInput) (interface{}, error) {
	serializerId, err := input.ReadInt32()
	if err != nil {
		return nil, err
	}
	serializer := service.registry[serializerId]
	return serializer.Read(input)
}

func (service *SerializationService) FindSerializerFor(obj interface{}) (Serializer, error) {
	var serializer Serializer
	if obj == nil {
		serializer = service.registry[service.nameToId["nil"]]
	}

	if serializer == nil {
		serializer = service.lookUpDefaultSerializer(obj)
	}

	if serializer == nil {
		serializer = service.lookUpCustomSerializer(obj)
	}

	if serializer == nil {
		serializer = service.lookUpGlobalSerializer()
	}

	if serializer == nil {
		//serializer=findSerializerByName('!json', false)
	}

	if serializer == nil {
		return nil, NewHazelcastSerializationError(fmt.Sprintf("there is no suitable serializer for %v", obj), nil)
	}
	return serializer, nil
}

func (service *SerializationService) registerDefaultSerializers() {
	service.registerSerializer(&ByteSerializer{})
	service.nameToId["uint8"] = CONSTANT_TYPE_BYTE

	service.registerSerializer(&BoolSerializer{})
	service.nameToId["bool"] = CONSTANT_TYPE_BOOLEAN

	service.registerSerializer(&UInteger16Serializer{})
	service.nameToId["uint16"] = CONSTANT_TYPE_CHAR

	service.registerSerializer(&Integer16Serializer{})
	service.nameToId["int16"] = CONSTANT_TYPE_SHORT

	service.registerSerializer(&Integer32Serializer{})
	service.nameToId["int32"] = CONSTANT_TYPE_INTEGER

	service.registerSerializer(&Integer64Serializer{})
	service.nameToId["int64"] = CONSTANT_TYPE_LONG

	service.registerSerializer(&Float32Serializer{})
	service.nameToId["float32"] = CONSTANT_TYPE_FLOAT

	service.registerSerializer(&Float64Serializer{})
	service.nameToId["float64"] = CONSTANT_TYPE_DOUBLE

	service.registerSerializer(&StringSerializer{})
	service.nameToId["string"] = CONSTANT_TYPE_STRING

	service.registerSerializer(&NilSerializer{})
	service.nameToId["nil"] = CONSTANT_TYPE_NULL

	service.registerSerializer(&ByteArraySerializer{})
	service.nameToId["[]uint8"] = CONSTANT_TYPE_BYTE_ARRAY

	service.registerSerializer(&BoolArraySerializer{})
	service.nameToId["[]bool"] = CONSTANT_TYPE_BOOLEAN_ARRAY

	service.registerSerializer(&UInteger16ArraySerializer{})
	service.nameToId["[]uint16"] = CONSTANT_TYPE_CHAR_ARRAY

	service.registerSerializer(&Integer16ArraySerializer{})
	service.nameToId["[]int16"] = CONSTANT_TYPE_SHORT_ARRAY

	service.registerSerializer(&Integer32ArraySerializer{})
	service.nameToId["[]int32"] = CONSTANT_TYPE_INTEGER_ARRAY

	service.registerSerializer(&Integer64ArraySerializer{})
	service.nameToId["[]int64"] = CONSTANT_TYPE_LONG_ARRAY

	service.registerSerializer(&Float32ArraySerializer{})
	service.nameToId["[]float32"] = CONSTANT_TYPE_FLOAT_ARRAY

	service.registerSerializer(&Float64ArraySerializer{})
	service.nameToId["[]float64"] = CONSTANT_TYPE_DOUBLE_ARRAY

	service.registerSerializer(&StringArraySerializer{})
	service.nameToId["[]string"] = CONSTANT_TYPE_STRING_ARRAY

	service.registerIdentifiedFactories()

	service.registerSerializer(NewPortableSerializer(service, service.serializationConfig.PortableFactories(), service.serializationConfig.PortableVersion()))
	service.nameToId["!portable"] = CONSTANT_TYPE_PORTABLE

}

func (service *SerializationService) registerCustomSerializers(customSerializers map[reflect.Type]Serializer) {
	for _, customSerializer := range customSerializers {
		service.registerSerializer(customSerializer)
	}
}

func (service *SerializationService) registerSerializer(serializer Serializer) error {
	if service.registry[serializer.Id()] != nil {
		return NewHazelcastSerializationError("this serializer is already in the registry", nil)
	}
	service.registry[serializer.Id()] = serializer
	return nil
}

func (service *SerializationService) registerGlobalSerializer(globalSerializer Serializer) {
	if globalSerializer != nil {
		service.registerSerializer(globalSerializer)
	}
}

func (service *SerializationService) getIdByObject(obj interface{}) *int32 {
	if val, ok := service.nameToId[reflect.TypeOf(obj).String()]; ok {
		return &val
	}
	return nil
}

func (service *SerializationService) lookUpDefaultSerializer(obj interface{}) Serializer {
	var serializer Serializer
	if isIdentifiedDataSerializable(obj) {
		return service.registry[service.nameToId["identified"]]
	}
	if isPortableSerializable(obj) {
		return service.registry[service.nameToId["!portable"]]
	}

	if service.getIdByObject(obj) == nil {
		return nil
	}

	serializer = service.registry[*service.getIdByObject(obj)]

	return serializer
}

func (service *SerializationService) lookUpCustomSerializer(obj interface{}) Serializer {
	for key, val := range service.serializationConfig.CustomSerializers() {
		if key.Kind() == reflect.Interface {
			if reflect.TypeOf(obj).Implements(key) {
				return val
			}
		} else {
			if reflect.TypeOf(obj) == key {
				return val
			}
		}
	}
	return nil
}

func (service *SerializationService) lookUpGlobalSerializer() Serializer {
	return service.serializationConfig.GlobalSerializer()
}

func (service *SerializationService) registerIdentifiedFactories() {
	factories := make(map[int32]IdentifiedDataSerializableFactory)
	for id, _ := range service.serializationConfig.DataSerializableFactories() {
		factories[id] = service.serializationConfig.DataSerializableFactories()[id]
	}

	idToPredicate := make(map[int32]IdentifiedDataSerializable)
	fillPredicateIds(idToPredicate)
	factories[PREDICATE_FACTORY_ID] = NewPredicateFactory(idToPredicate)

	//factories[RELIABLE_TOPIC_MESSAGE_FACTORY_ID] = new ReliableTopicMessageFactory()
	//factories[CLUSTER_DATA_FACTORY_ID] = new ClusterDataFactory()
	service.registerSerializer(NewIdentifiedDataSerializableSerializer(factories))
	service.nameToId["identified"] = CONSTANT_TYPE_DATA_SERIALIZABLE
}

func fillPredicateIds(idToPredicate map[int32]IdentifiedDataSerializable) {
	idToPredicate[SQL_PREDICATE] = &SqlPredicate{}
	idToPredicate[AND_PREDICATE] = &AndPredicate{}
	idToPredicate[BETWEEN_PREDICATE] = &BetweenPredicate{}
	idToPredicate[EQUAL_PREDICATE] = &EqualPredicate{}
	idToPredicate[GREATERLESS_PREDICATE] = &GreaterLessPredicate{}
	idToPredicate[LIKE_PREDICATE] = &LikePredicate{}
	idToPredicate[ILIKE_PREDICATE] = &ILikePredicate{}
	idToPredicate[IN_PREDICATE] = &InPredicate{}
	idToPredicate[INSTANCEOF_PREDICATE] = &InstanceOfPredicate{}
	idToPredicate[NOTEQUAL_PREDICATE] = &NotEqualPredicate{}
	idToPredicate[NOT_PREDICATE] = &NotPredicate{}
	idToPredicate[OR_PREDICATE] = &OrPredicate{}
	idToPredicate[REGEX_PREDICATE] = &RegexPredicate{}
	idToPredicate[FALSE_PREDICATE] = &FalsePredicate{}
	idToPredicate[TRUE_PREDICATE] = &TruePredicate{}
}

func isIdentifiedDataSerializable(obj interface{}) bool {
	_, ok := obj.(IdentifiedDataSerializable)
	return ok
}

func isPortableSerializable(obj interface{}) bool {
	_, ok := obj.(Portable)
	return ok
}
