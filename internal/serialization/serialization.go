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
	"reflect"
	"strconv"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/predicates"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

////// SerializationService ///////////
type SerializationService struct {
	serializationConfig *config.SerializationConfig
	registry            map[int32]serialization.Serializer
	nameToId            map[string]int32
}

func NewSerializationService(serializationConfig *config.SerializationConfig) *SerializationService {
	v1 := SerializationService{serializationConfig: serializationConfig, nameToId: make(map[string]int32), registry: make(map[int32]serialization.Serializer)}
	v1.registerDefaultSerializers()
	v1.registerCustomSerializers(serializationConfig.CustomSerializers())
	v1.registerGlobalSerializer(serializationConfig.GlobalSerializer())
	return &v1
}

func (s *SerializationService) ToData(object interface{}) (*Data, error) {
	if _, ok := object.(*Data); ok {
		return object.(*Data), nil
	}
	dataOutput := NewPositionalObjectDataOutput(1, s, s.serializationConfig.IsBigEndian())
	serializer, err := s.FindSerializerFor(object)
	if err != nil {
		return nil, err
	}
	dataOutput.WriteInt32(0) // partition
	dataOutput.WriteInt32(serializer.Id())
	err = serializer.Write(dataOutput, object)
	return &Data{dataOutput.buffer}, err
}

func (s *SerializationService) ToObject(data *Data) (interface{}, error) {
	if data == nil {
		return nil, nil
	}
	if data.GetType() == 0 {
		return data, nil
	}
	var serializer = s.registry[data.GetType()]
	dataInput := NewObjectDataInput(data.Buffer(), DataOffset, s, s.serializationConfig.IsBigEndian())
	return serializer.Read(dataInput)
}

func (s *SerializationService) WriteObject(output serialization.DataOutput, object interface{}) error {
	var serializer, err = s.FindSerializerFor(object)
	if err != nil {
		return err
	}
	output.WriteInt32(serializer.Id())
	return serializer.Write(output, object)
}

func (s *SerializationService) ReadObject(input serialization.DataInput) (interface{}, error) {
	serializerId, err := input.ReadInt32()
	if err != nil {
		return nil, err
	}
	serializer := s.registry[serializerId]
	return serializer.Read(input)
}

func (s *SerializationService) FindSerializerFor(obj interface{}) (serialization.Serializer, error) {
	var serializer serialization.Serializer
	if obj == nil {
		serializer = s.registry[s.nameToId["nil"]]
	}

	if serializer == nil {
		serializer = s.lookUpDefaultSerializer(obj)
	}

	if serializer == nil {
		serializer = s.lookUpCustomSerializer(obj)
	}

	if serializer == nil {
		serializer = s.lookUpGlobalSerializer()
	}

	if serializer == nil {
		serializer = s.registry[s.nameToId["!gob"]]
	}

	if serializer == nil {
		return nil, core.NewHazelcastSerializationError(fmt.Sprintf("there is no suitable serializer for %v", obj), nil)
	}
	return serializer, nil
}

func (s *SerializationService) registerDefaultSerializers() {
	s.registerSerializer(&ByteSerializer{})
	s.nameToId["uint8"] = ConstantTypeByte

	s.registerSerializer(&BoolSerializer{})
	s.nameToId["bool"] = ConstantTypeBool

	s.registerSerializer(&UInteger16Serializer{})
	s.nameToId["uint16"] = ConstantTypeUInteger16

	s.registerSerializer(&Integer16Serializer{})
	s.nameToId["int16"] = ConstantTypeInteger16

	s.registerSerializer(&Integer32Serializer{})
	s.nameToId["int32"] = ConstantTypeInteger32

	s.registerSerializer(&Integer64Serializer{})
	s.nameToId["int64"] = ConstantTypeInteger64

	s.registerSerializer(&Float32Serializer{})
	s.nameToId["float32"] = ConstantTypeFloat32

	s.registerSerializer(&Float64Serializer{})
	s.nameToId["float64"] = ConstantTypeFloat64

	s.registerSerializer(&StringSerializer{})
	s.nameToId["string"] = ConstantTypeString

	s.registerSerializer(&NilSerializer{})
	s.nameToId["nil"] = ConstantTypeNil

	s.registerSerializer(&ByteArraySerializer{})
	s.nameToId["[]uint8"] = ConstantTypeByteArray

	s.registerSerializer(&BoolArraySerializer{})
	s.nameToId["[]bool"] = ConstantTypeBoolArray

	s.registerSerializer(&UInteger16ArraySerializer{})
	s.nameToId["[]uint16"] = ConstantTypeUInteger16Array

	s.registerSerializer(&Integer16ArraySerializer{})
	s.nameToId["[]int16"] = ConstantTypeInteger16Array

	s.registerSerializer(&Integer32ArraySerializer{})
	s.nameToId["[]int32"] = ConstantTypeInteger32Array

	s.registerSerializer(&Integer64ArraySerializer{})
	s.nameToId["[]int64"] = ConstantTypeInteger64Array

	s.registerSerializer(&Float32ArraySerializer{})
	s.nameToId["[]float32"] = ConstantTypeFloat32Array

	s.registerSerializer(&Float64ArraySerializer{})
	s.nameToId["[]float64"] = ConstantTypeFloat64Array

	s.registerSerializer(&StringArraySerializer{})
	s.nameToId["[]string"] = ConstantTypeStringArray

	s.registerSerializer(&GobSerializer{})
	s.nameToId["!gob"] = GoGobSerializationType

	s.registerIdentifiedFactories()

	portableSerializer := NewPortableSerializer(s, s.serializationConfig.PortableFactories(), s.serializationConfig.PortableVersion())

	s.registerClassDefinitions(portableSerializer, s.serializationConfig.ClassDefinitions())
	s.registerSerializer(portableSerializer)
	s.nameToId["!portable"] = ConstantTypePortable

}

func (s *SerializationService) registerCustomSerializers(customSerializers map[reflect.Type]serialization.Serializer) {
	for _, customSerializer := range customSerializers {
		s.registerSerializer(customSerializer)
	}
}

func (s *SerializationService) registerSerializer(serializer serialization.Serializer) error {
	if s.registry[serializer.Id()] != nil {
		return core.NewHazelcastSerializationError("this serializer is already in the registry", nil)
	}
	s.registry[serializer.Id()] = serializer
	return nil
}

func (s *SerializationService) registerClassDefinitions(portableSerializer *PortableSerializer, classDefinitions []serialization.ClassDefinition) {
	for _, cd := range classDefinitions {
		portableSerializer.portableContext.RegisterClassDefinition(cd)
	}
}

func (s *SerializationService) registerGlobalSerializer(globalSerializer serialization.Serializer) {
	if globalSerializer != nil {
		s.registerSerializer(globalSerializer)
	}
}

func (s *SerializationService) getIdByObject(obj interface{}) *int32 {
	typ := reflect.TypeOf(obj).String()
	if typ == "int" || typ == "[]int" {
		typ = typ + strconv.Itoa(64)
	}
	if val, ok := s.nameToId[typ]; ok {
		return &val
	}
	return nil
}

func (s *SerializationService) lookUpDefaultSerializer(obj interface{}) serialization.Serializer {
	var serializer serialization.Serializer
	if isIdentifiedDataSerializable(obj) {
		return s.registry[s.nameToId["identified"]]
	}
	if isPortableSerializable(obj) {
		return s.registry[s.nameToId["!portable"]]
	}

	if s.getIdByObject(obj) == nil {
		return nil
	}

	serializer = s.registry[*s.getIdByObject(obj)]

	return serializer
}

func (s *SerializationService) lookUpCustomSerializer(obj interface{}) serialization.Serializer {
	for key, val := range s.serializationConfig.CustomSerializers() {
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

func (s *SerializationService) lookUpGlobalSerializer() serialization.Serializer {
	return s.serializationConfig.GlobalSerializer()
}

func (s *SerializationService) registerIdentifiedFactories() {
	factories := make(map[int32]serialization.IdentifiedDataSerializableFactory)
	for id, _ := range s.serializationConfig.DataSerializableFactories() {
		factories[id] = s.serializationConfig.DataSerializableFactories()[id]
	}
	factories[predicates.PredicateFactoryId] = predicates.NewPredicateFactory()

	//factories[RELIABLE_TOPIC_MESSAGE_FACTORY_ID] = new ReliableTopicMessageFactory()
	//factories[CLUSTER_DATA_FACTORY_ID] = new ClusterDataFactory()
	s.registerSerializer(NewIdentifiedDataSerializableSerializer(factories))
	s.nameToId["identified"] = ConstantTypeDataSerializable
}

func isIdentifiedDataSerializable(obj interface{}) bool {
	_, ok := obj.(serialization.IdentifiedDataSerializable)
	return ok
}

func isPortableSerializable(obj interface{}) bool {
	_, ok := obj.(serialization.Portable)
	return ok
}
