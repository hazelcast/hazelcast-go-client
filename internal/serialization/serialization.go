// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serialization

import (
	"fmt"
	. "github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/predicates"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	"reflect"
	"strconv"
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
	if _, ok := object.(*Data); ok {
		return object.(*Data), nil
	}
	dataOutput := NewPositionalObjectDataOutput(1, service, service.serializationConfig.IsBigEndian())
	serializer, err := service.FindSerializerFor(object)
	if err != nil {
		return nil, err
	}
	dataOutput.WriteInt32(0) // partition
	dataOutput.WriteInt32(serializer.Id())
	err = serializer.Write(dataOutput, object)
	return &Data{dataOutput.buffer}, err
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
	return serializer.Write(output, object)
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
		serializer = service.registry[service.nameToId["!gob"]]
	}

	if serializer == nil {
		return nil, core.NewHazelcastSerializationError(fmt.Sprintf("there is no suitable serializer for %v", obj), nil)
	}
	return serializer, nil
}

func (service *SerializationService) registerDefaultSerializers() {
	service.registerSerializer(&ByteSerializer{})
	service.nameToId["uint8"] = CONSTANT_TYPE_BYTE

	service.registerSerializer(&BoolSerializer{})
	service.nameToId["bool"] = CONSTANT_TYPE_BOOL

	service.registerSerializer(&UInteger16Serializer{})
	service.nameToId["uint16"] = CONSTANT_TYPE_UINTEGER16

	service.registerSerializer(&Integer16Serializer{})
	service.nameToId["int16"] = CONSTANT_TYPE_INTEGER16

	service.registerSerializer(&Integer32Serializer{})
	service.nameToId["int32"] = CONSTANT_TYPE_INTEGER32

	service.registerSerializer(&Integer64Serializer{})
	service.nameToId["int64"] = CONSTANT_TYPE_INTEGER64

	service.registerSerializer(&Float32Serializer{})
	service.nameToId["float32"] = CONSTANT_TYPE_FLOAT32

	service.registerSerializer(&Float64Serializer{})
	service.nameToId["float64"] = CONSTANT_TYPE_FLOAT64

	service.registerSerializer(&StringSerializer{})
	service.nameToId["string"] = CONSTANT_TYPE_STRING

	service.registerSerializer(&NilSerializer{})
	service.nameToId["nil"] = CONSTANT_TYPE_NULL

	service.registerSerializer(&ByteArraySerializer{})
	service.nameToId["[]uint8"] = CONSTANT_TYPE_BYTE_ARRAY

	service.registerSerializer(&BoolArraySerializer{})
	service.nameToId["[]bool"] = CONSTANT_TYPE_BOOL_ARRAY

	service.registerSerializer(&UInteger16ArraySerializer{})
	service.nameToId["[]uint16"] = CONSTANT_TYPE_UINTEGER16_ARRAY

	service.registerSerializer(&Integer16ArraySerializer{})
	service.nameToId["[]int16"] = CONSTANT_TYPE_INTEGER16_ARRAY

	service.registerSerializer(&Integer32ArraySerializer{})
	service.nameToId["[]int32"] = CONSTANT_TYPE_INTEGER32_ARRAY

	service.registerSerializer(&Integer64ArraySerializer{})
	service.nameToId["[]int64"] = CONSTANT_TYPE_INTEGER64_ARRAY

	service.registerSerializer(&Float32ArraySerializer{})
	service.nameToId["[]float32"] = CONSTANT_TYPE_FLOAT32_ARRAY

	service.registerSerializer(&Float64ArraySerializer{})
	service.nameToId["[]float64"] = CONSTANT_TYPE_FLOAT64_ARRAY

	service.registerSerializer(&StringArraySerializer{})
	service.nameToId["[]string"] = CONSTANT_TYPE_STRING_ARRAY

	service.registerSerializer(&GobSerializer{})
	service.nameToId["!gob"] = GO_GOB_SERIALIZATION_TYPE

	service.registerIdentifiedFactories()

	portableSerializer := NewPortableSerializer(service, service.serializationConfig.PortableFactories(), service.serializationConfig.PortableVersion())

	service.registerClassDefinitions(portableSerializer, service.serializationConfig.ClassDefinitions())
	service.registerSerializer(portableSerializer)
	service.nameToId["!portable"] = CONSTANT_TYPE_PORTABLE

}

func (service *SerializationService) registerCustomSerializers(customSerializers map[reflect.Type]Serializer) {
	for _, customSerializer := range customSerializers {
		service.registerSerializer(customSerializer)
	}
}

func (service *SerializationService) registerSerializer(serializer Serializer) error {
	if service.registry[serializer.Id()] != nil {
		return core.NewHazelcastSerializationError("this serializer is already in the registry", nil)
	}
	service.registry[serializer.Id()] = serializer
	return nil
}

func (service *SerializationService) registerClassDefinitions(portableSerializer *PortableSerializer, classDefinitions []ClassDefinition) {
	for _, cd := range classDefinitions {
		portableSerializer.portableContext.RegisterClassDefinition(cd)
	}
}

func (service *SerializationService) registerGlobalSerializer(globalSerializer Serializer) {
	if globalSerializer != nil {
		service.registerSerializer(globalSerializer)
	}
}

func (service *SerializationService) getIdByObject(obj interface{}) *int32 {
	typ := reflect.TypeOf(obj).String()
	if typ == "int" || typ == "[]int" {
		typ = typ + strconv.Itoa(64)
	}
	if val, ok := service.nameToId[typ]; ok {
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
	factories[PREDICATE_FACTORY_ID] = NewPredicateFactory()

	//factories[RELIABLE_TOPIC_MESSAGE_FACTORY_ID] = new ReliableTopicMessageFactory()
	//factories[CLUSTER_DATA_FACTORY_ID] = new ClusterDataFactory()
	service.registerSerializer(NewIdentifiedDataSerializableSerializer(factories))
	service.nameToId["identified"] = CONSTANT_TYPE_DATA_SERIALIZABLE
}

func isIdentifiedDataSerializable(obj interface{}) bool {
	_, ok := obj.(IdentifiedDataSerializable)
	return ok
}

func isPortableSerializable(obj interface{}) bool {
	_, ok := obj.(Portable)
	return ok
}
