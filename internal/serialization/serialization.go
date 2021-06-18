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
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
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
func (s *Service) ToData(object interface{}) (r *Data, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = makeError(rec)
		}
	}()
	if serData, ok := object.(*Data); ok {
		return serData, nil
	}
	// initial size is kept minimal (head_data_offset + long_size), since it'll grow on demand
	dataOutput := NewPositionalObjectDataOutput(16, s, s.SerializationConfig.BigEndian)
	serializer, err := s.FindSerializerFor(object)
	if err != nil {
		return nil, err
	}
	dataOutput.WriteInt32(0) // partition
	dataOutput.WriteInt32(serializer.ID())
	serializer.Write(dataOutput, object)
	return &Data{dataOutput.buffer[:dataOutput.position]}, err
}

// ToObject deserializes the given Data to an object.
// It can safely be called on an object that is already deserialized. In that case, that instance
// is returned.
// If this is called with nil, nil is returned.
func (s *Service) ToObject(data *Data) (r interface{}, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = makeError(rec)
		}
	}()
	var ok bool
	if data == nil {
		return nil, nil
	}
	typeID := data.Type()
	if typeID == 0 {
		return data, nil
	}
	serializer := s.lookupBuiltinDeserializer(typeID)
	if serializer == nil {
		serializer, ok = s.registry[typeID]
		if !ok {
			return nil, hzerrors.NewHazelcastSerializationError(fmt.Sprintf("there is no suitable de-serializer for type %d", typeID), nil)
		}
	}
	dataInput := NewObjectDataInput(data.Buffer(), DataOffset, s, s.SerializationConfig.BigEndian)
	return serializer.Read(dataInput), nil
}

func (s *Service) WriteObject(output pubserialization.DataOutput, object interface{}) {
	serializer, err := s.FindSerializerFor(object)
	if err != nil {
		panic(fmt.Errorf("error finding serializer: %w", err))
	}
	output.WriteInt32(serializer.ID())
	serializer.Write(output, object)
}

func (s *Service) ReadObject(input pubserialization.DataInput) interface{} {
	serializerID := input.ReadInt32()
	serializer := s.registry[serializerID]
	return serializer.Read(input)
}

func (s *Service) FindSerializerFor(obj interface{}) (pubserialization.Serializer, error) {
	serializer := s.LookUpDefaultSerializer(obj)
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

func (s *Service) LookUpDefaultSerializer(obj interface{}) pubserialization.Serializer {
	serializer := s.lookupBuiltinSerializer(obj)
	if serializer != nil {
		return serializer
	}
	if _, ok := obj.(pubserialization.IdentifiedDataSerializable); ok {
		return s.registry[s.nameToID["identified"]]
	}
	if _, ok := obj.(pubserialization.Portable); ok {
		return s.registry[s.nameToID["!portable"]]
	}
	return nil
}

func (s *Service) lookupBuiltinDeserializer(typeID int32) pubserialization.Serializer {
	switch typeID {
	case TypeNil:
		return nilSerializer
	case TypeBool:
		return boolSerializer
	case TypeString:
		return stringSerializer
	case TypeByte:
		return uint8Serializer
	case TypeUInt16:
		return uint16Serializer
	case TypeInt16:
		return int16Serializer
	case TypeInt32:
		return int32Serializer
	case TypeInt64:
		return int64Serializer
	case TypeFloat32:
		return float32Serializer
	case TypeFloat64:
		return float64Serializer
	case TypeBoolArray:
		return boolArraySerializer
	case TypeStringArray:
		return stringArraySerializer
	case TypeByteArray:
		return uint8ArraySerializer
	case TypeUInt16Array:
		return uint16ArraySerializer
	case TypeInt16Array:
		return int16ArraySerializer
	case TypeInt32Array:
		return int32ArraySerializer
	case TypeInt64Array:
		return int64ArraySerializer
	case TypeFloat32Array:
		return float32ArraySerializer
	case TypeFloat64Array:
		return float64ArraySerializer
	case TypeUUID:
		return uuidSerializer
	case TypeJavaDate:
		return javaDateSerializer
	case TypeJavaBigInteger:
		return javaBigIntSerializer
	case TypeJSONSerialization:
		return jsonSerializer
	}
	return nil
}

func (s *Service) registerDefaultSerializers() error {
	if err := s.registerIdentifiedFactories(); err != nil {
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
	s.nameToID["!portable"] = TypePortable
	return nil
}

func (s *Service) registerCustomSerializers(customSerializers map[reflect.Type]pubserialization.Serializer) {
	for _, customSerializer := range customSerializers {
		if err := s.registerSerializer(customSerializer); err != nil {
			panic(err)
		}
	}
}

func (s *Service) registerSerializer(serializer pubserialization.Serializer) error {
	if s.registry[serializer.ID()] != nil {
		return hzerrors.NewHazelcastSerializationError("this serializer is already in the registry", nil)
	}
	s.registry[serializer.ID()] = serializer
	return nil
}

func (s *Service) registerClassDefinitions(serializer *PortableSerializer, classDefs []*pubserialization.ClassDefinition) {
	for _, cd := range classDefs {
		if err := serializer.portableContext.RegisterClassDefinition(cd); err != nil {
			panic(err)
		}
	}
}

func (s *Service) registerGlobalSerializer(globalSerializer pubserialization.Serializer) {
	if globalSerializer != nil {
		if err := s.registerSerializer(globalSerializer); err != nil {
			panic(err)
		}
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
	s.nameToID["identified"] = TypeDataSerializable
	return nil
}

func (s *Service) lookupBuiltinSerializer(obj interface{}) pubserialization.Serializer {
	switch obj.(type) {
	case nil:
		return nilSerializer
	case bool:
		return boolSerializer
	case string:
		return stringSerializer
	case uint8:
		return uint8Serializer
	case uint16:
		return uint16Serializer
	case int:
		return intSerializer
	case int16:
		return int16Serializer
	case int32:
		return int32Serializer
	case int64:
		return int64Serializer
	case float32:
		return float32Serializer
	case float64:
		return float64Serializer
	case []bool:
		return boolArraySerializer
	case []string:
		return stringArraySerializer
	case []uint8:
		return uint8ArraySerializer
	case []uint16:
		return uint16ArraySerializer
	case []int16:
		return int16ArraySerializer
	case []int32:
		return int32ArraySerializer
	case []int64:
		return int64ArraySerializer
	case []float32:
		return float32ArraySerializer
	case []float64:
		return float64ArraySerializer
	case types.UUID:
		return uuidSerializer
	case time.Time:
		return javaDateSerializer
	case *big.Int:
		return javaBigIntSerializer
	case pubserialization.JSON:
		return jsonSerializer
	}
	return nil
}

func makeError(rec interface{}) error {
	switch v := rec.(type) {
	case error:
		return v
	case string:
		return errors.New(v)
	default:
		return fmt.Errorf("%v", rec)
	}
}

var nilSerializer = &NilSerializer{}
var boolSerializer = &BoolSerializer{}
var stringSerializer = &StringSerializer{}
var uint8Serializer = &ByteSerializer{}
var uint16Serializer = &UInt16Serializer{}
var intSerializer = &IntSerializer{}
var int16Serializer = &Int16Serializer{}
var int32Serializer = &Int32Serializer{}
var int64Serializer = &Int64Serializer{}
var float32Serializer = &Float32Serializer{}
var float64Serializer = &Float64Serializer{}
var boolArraySerializer = &BoolArraySerializer{}
var stringArraySerializer = &StringArraySerializer{}
var uint8ArraySerializer = &ByteArraySerializer{}
var uint16ArraySerializer = &UInt16ArraySerializer{}
var int16ArraySerializer = &Int16ArraySerializer{}
var int32ArraySerializer = &Int32ArraySerializer{}
var int64ArraySerializer = &Int64ArraySerializer{}
var float32ArraySerializer = &Float32ArraySerializer{}
var float64ArraySerializer = &Float64ArraySerializer{}
var uuidSerializer = &UUIDSerializer{}
var jsonSerializer = &JSONValueSerializer{}
var javaDateSerializer = &JavaDateSerializer{}
var javaBigIntSerializer = &JavaBigIntegerSerializer{}
var gobSerializer = &GobSerializer{}
