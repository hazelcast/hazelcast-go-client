/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package serialization

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

// Service serializes user objects to Data and back to Object.
// Data is the internal representation of binary Data in Hazelcast.
type Service struct {
	SerializationConfig *pubserialization.Config
	registry            map[int32]pubserialization.Serializer
	nameToID            map[string]int32
}

func NewService(serializationConfig *pubserialization.Config) (*Service, error) {
	v1 := Service{
		SerializationConfig: serializationConfig,
		nameToID:            make(map[string]int32),
		registry:            make(map[int32]pubserialization.Serializer),
	}
	err := v1.registerDefaultSerializers()
	if err != nil {
		return nil, err
	}
	v1.registerCustomSerializers(serializationConfig.CustomSerializers)
	v1.registerGlobalSerializer(serializationConfig.GlobalSerializer)
	return &v1, nil
}

// ToData serializes an object to a Data.
// It can safely be called with a Data. In that case, that instance is returned.
// If it is called with nil, nil is returned.
func (s *Service) ToData(object interface{}) (pubserialization.Data, error) {
	if _, ok := object.(*SerializationData); ok {
		return object.(*SerializationData), nil
	}
	dataOutput := NewPositionalObjectDataOutput(1, s, s.SerializationConfig.BigEndian)
	serializer, err := s.FindSerializerFor(object)
	if err != nil {
		return nil, err
	}
	dataOutput.WriteInt32(0) // partition
	dataOutput.WriteInt32(serializer.ID())
	err = serializer.Write(dataOutput, object)
	return &SerializationData{dataOutput.buffer[:dataOutput.position]}, err
}

// ToObject deserializes the given Data to an object.
// It can safely be called on an object that is already deserialized. In that case, that instance
// is returned.
// If this is called with nil, nil is returned.
func (s *Service) ToObject(data pubserialization.Data) (interface{}, error) {
	if data == nil {
		return nil, nil
	}
	typeID := data.Type()
	if typeID == 0 {
		return data, nil
	}
	serializer, ok := s.registry[typeID]
	if !ok {
		return nil, hzerrors.NewHazelcastSerializationError(fmt.Sprintf("there is no suitable de-serializer for type %d", typeID), nil)
	}
	dataInput := NewObjectDataInput(data.Buffer(), DataOffset, s, s.SerializationConfig.BigEndian)
	return serializer.Read(dataInput)
}

func (s *Service) WriteObject(output pubserialization.DataOutput, object interface{}) error {
	serializer, err := s.FindSerializerFor(object)
	if err != nil {
		return err
	}
	output.WriteInt32(serializer.ID())
	return serializer.Write(output, object)
}

func (s *Service) ReadObject(input pubserialization.DataInput) (interface{}, error) {
	serializerID := input.ReadInt32()
	if input.Error() != nil {
		return nil, input.Error()
	}
	serializer := s.registry[serializerID]
	return serializer.Read(input)
}

func (s *Service) FindSerializerFor(obj interface{}) (pubserialization.Serializer, error) {
	var serializer pubserialization.Serializer
	if obj == nil {
		serializer = s.registry[s.nameToID["nil"]]
	}
	if serializer == nil {
		serializer = s.LookUpDefaultSerializer(obj)
	}
	if serializer == nil {
		serializer = s.lookUpCustomSerializer(obj)
	}
	if serializer == nil {
		serializer = s.lookUpGlobalSerializer()
	}
	if serializer == nil {
		serializer = s.registry[s.nameToID["!gob"]]
	}
	if serializer == nil {
		return nil, hzerrors.NewHazelcastSerializationError(fmt.Sprintf("there is no suitable serializer for %v", obj), nil)
	}
	return serializer, nil
}

func (s *Service) registerDefaultSerializers() error {
	sers := []struct {
		l string
		i int32
		s pubserialization.Serializer
	}{
		{l: "uint8", i: ConstantTypeByte, s: &ByteSerializer{}},
		{l: "bool", i: ConstantTypeBool, s: &BoolSerializer{}},
		{l: "uint16", i: ConstantTypeUInteger16, s: &UInteger16Serializer{}},
		{l: "int16", i: ConstantTypeInteger16, s: &Integer16Serializer{}},
		{l: "int32", i: ConstantTypeInteger32, s: &Integer32Serializer{}},
		{l: "int64", i: ConstantTypeInteger64, s: &Integer64Serializer{}},
		{l: "float32", i: ConstantTypeFloat32, s: &Float32Serializer{}},
		{l: "float64", i: ConstantTypeFloat64, s: &Float64Serializer{}},
		{l: "string", i: ConstantTypeString, s: &StringSerializer{}},
		{l: "nil", i: ConstantTypeNil, s: &NilSerializer{}},
		{l: "[]uint8", i: ConstantTypeByteArray, s: &ByteArraySerializer{}},
		{l: "[]bool", i: ConstantTypeBoolArray, s: &BoolArraySerializer{}},
		{l: "[]uint16", i: ConstantTypeUInteger16Array, s: &UInteger16ArraySerializer{}},
		{l: "[]int16", i: ConstantTypeInteger16Array, s: &Integer16ArraySerializer{}},
		{l: "[]int32", i: ConstantTypeInteger32Array, s: &Integer32ArraySerializer{}},
		{l: "[]int64", i: ConstantTypeInteger64Array, s: &Integer64ArraySerializer{}},
		{l: "[]float32", i: ConstantTypeFloat32Array, s: &Float32ArraySerializer{}},
		{l: "[]float64", i: ConstantTypeFloat64Array, s: &Float64ArraySerializer{}},
		{l: "[]string", i: ConstantTypeStringArray, s: &StringArraySerializer{}},
		{l: "serialization.JSON", i: JSONSerializationType, s: &JSONValueSerializer{}},
		{l: "!gob", i: GoGobSerializationType, s: &GobSerializer{}},
	}
	for _, ser := range sers {
		if err := s.registerSerializer(ser.s); err != nil {
			return err
		}
		s.nameToID[ser.l] = ser.i
	}
	err := s.registerIdentifiedFactories()
	if err != nil {
		return err
	}
	portableSerializer, err := NewPortableSerializer(s, s.SerializationConfig.PortableFactories, s.SerializationConfig.PortableVersion)
	if err != nil {
		return err
	}
	s.registerClassDefinitions(portableSerializer, s.SerializationConfig.ClassDefinitions)
	if err = s.registerSerializer(portableSerializer); err != nil {
		return err
	}
	s.nameToID["!portable"] = ConstantTypePortable
	return nil

}

func (s *Service) registerCustomSerializers(customSerializers map[reflect.Type]pubserialization.Serializer) {
	for _, customSerializer := range customSerializers {
		s.registerSerializer(customSerializer)
	}
}

func (s *Service) registerSerializer(serializer pubserialization.Serializer) error {
	if s.registry[serializer.ID()] != nil {
		return hzerrors.NewHazelcastSerializationError("this serializer is already in the registry", nil)
	}
	s.registry[serializer.ID()] = serializer
	return nil
}

func (s *Service) registerClassDefinitions(portableSerializer *PortableSerializer,
	classDefinitions []pubserialization.ClassDefinition) {
	for _, cd := range classDefinitions {
		portableSerializer.portableContext.RegisterClassDefinition(cd)
	}
}

func (s *Service) registerGlobalSerializer(globalSerializer pubserialization.Serializer) {
	if globalSerializer != nil {
		s.registerSerializer(globalSerializer)
	}
}

func (s *Service) getIDByObject(obj interface{}) (int32, bool) {
	typ := reflect.TypeOf(obj).String()
	if typ == "int" || typ == "[]int" {
		typ = typ + strconv.Itoa(64)
	}
	if val, ok := s.nameToID[typ]; ok {
		return val, true
	}
	return 0, false
}

func (s *Service) LookUpDefaultSerializer(obj interface{}) pubserialization.Serializer {
	var serializer pubserialization.Serializer
	if isIdentifiedDataSerializable(obj) {
		return s.registry[s.nameToID["identified"]]
	}
	if isPortableSerializable(obj) {
		return s.registry[s.nameToID["!portable"]]
	}
	id, found := s.getIDByObject(obj)
	if !found {
		return nil
	}
	serializer = s.registry[id]
	return serializer
}

func (s *Service) lookUpCustomSerializer(obj interface{}) pubserialization.Serializer {
	for key, val := range s.SerializationConfig.CustomSerializers {
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

func (s *Service) lookUpGlobalSerializer() pubserialization.Serializer {
	return s.SerializationConfig.GlobalSerializer
}

func (s *Service) registerIdentifiedFactories() error {
	factories := make(map[int32]pubserialization.IdentifiedDataSerializableFactory)
	fs := map[int32]pubserialization.IdentifiedDataSerializableFactory{}
	for _, f := range s.SerializationConfig.IdentifiedDataSerializableFactories {
		fid := f.FactoryID()
		if _, ok := fs[fid]; ok {
			return hzerrors.NewHazelcastSerializationError("this serializer is already in the registry", nil)
		}
		fs[fid] = f
	}
	if err := s.registerSerializer(NewIdentifiedDataSerializableSerializer(factories)); err != nil {
		return err
	}
	s.nameToID["identified"] = ConstantTypeDataSerializable
	return nil
}

func isIdentifiedDataSerializable(obj interface{}) bool {
	_, ok := obj.(pubserialization.IdentifiedDataSerializable)
	return ok
}

func isPortableSerializable(obj interface{}) bool {
	_, ok := obj.(pubserialization.Portable)
	return ok
}
