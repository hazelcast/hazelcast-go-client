/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"math/big"
	"reflect"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/proxy"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

// Service serializes user objects to Data and back to Object.
// Data is the internal representation of binary Data in Hazelcast.
type Service struct {
	SerializationConfig  *pubserialization.Config
	registry             map[int32]pubserialization.Serializer
	portableSerializer   *PortableSerializer
	identifiedSerializer *IdentifiedDataSerializableSerializer
	customSerializers    map[reflect.Type]pubserialization.Serializer
	compactSerializer    *CompactStreamSerializer
}

func NewService(config *pubserialization.Config, schemaCh chan SchemaMsg) (*Service, error) {
	var err error
	s := &Service{
		SerializationConfig: config,
		registry:            make(map[int32]pubserialization.Serializer),
		customSerializers:   config.CustomSerializers(),
		compactSerializer:   NewCompactStreamSerializer(config.Compact, schemaCh),
	}
	s.portableSerializer, err = NewPortableSerializer(s, s.SerializationConfig.PortableFactories(), s.SerializationConfig.PortableVersion)
	if err != nil {
		return nil, err
	}
	s.registerClassDefinitions(s.portableSerializer, s.SerializationConfig.ClassDefinitions())
	s.registerCustomSerializers(config.CustomSerializers())
	s.registerGlobalSerializer(config.GlobalSerializer())
	if err = s.registerIdentifiedFactories(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Service) SchemaService() *SchemaService {
	return s.compactSerializer.ss
}

// SetSchemaService is used in tests
func (s *Service) SetSchemaService(ss *SchemaService) {
	s.compactSerializer.ss = ss
}

// ToData serializes an object to a Data.
// It can safely be called with a Data. In that case, that instance is returned.
// If it is called with nil, nil is returned.
func (s *Service) ToData(object interface{}) (r Data, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = makeError(rec)
		}
	}()
	if serData, ok := object.(Data); ok {
		return serData, nil
	}
	// initial size is kept minimal (head_data_offset + long_size), since it'll grow on demand
	dataOutput := NewPositionalObjectDataOutput(16, s, !s.SerializationConfig.LittleEndian)
	serializer, err := s.FindSerializerFor(object)
	if err != nil {
		return Data{}, err
	}
	dataOutput.WriteInt32BigEndian(0) // partition
	dataOutput.WriteInt32BigEndian(serializer.ID())
	serializer.Write(dataOutput, object)
	return dataOutput.buffer[:dataOutput.position], err
}

// ToObject deserializes the given Data to an object.
// nil is returned if called with nil.
func (s *Service) ToObject(data Data) (r interface{}, err error) {
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
	serializer := s.lookupBuiltinDeserializer(typeID)
	if serializer == nil {
		serializer, ok = s.registry[typeID]
		if !ok {
			return nil, ihzerrors.NewSerializationError(fmt.Sprintf("there is no suitable de-serializer for type %d", typeID), nil)
		}
	}
	dataInput := NewObjectDataInput(data, DataOffset, s, !s.SerializationConfig.LittleEndian)
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
	typeID := input.ReadInt32()
	if serializer := s.lookupBuiltinDeserializer(typeID); serializer != nil {
		return serializer.Read(input)
	}
	if serializer := s.registry[typeID]; serializer != nil {
		return serializer.Read(input)
	}
	panic(fmt.Sprintf("unknown type ID: %d", typeID))
}

func (s *Service) FindSerializerFor(obj interface{}) (pubserialization.Serializer, error) {
	if serializer := s.LookUpDefaultSerializer(obj); serializer != (pubserialization.Serializer)(nil) {
		return serializer, nil
	}
	if serializer := s.lookUpCustomSerializer(obj); serializer != (pubserialization.Serializer)(nil) {
		return serializer, nil
	}
	if serializer := s.lookUpGlobalSerializer(); serializer != (pubserialization.Serializer)(nil) {
		return serializer, nil
	}
	// keeping the error in the result for future behavior change
	return gobSerializer, nil
}

func (s *Service) LookUpDefaultSerializer(obj interface{}) pubserialization.Serializer {
	serializer := s.lookupBuiltinSerializer(obj)
	if serializer != (pubserialization.Serializer)(nil) {
		return serializer
	}
	if s.compactSerializer.IsRegisteredAsCompact(reflect.TypeOf(obj)) {
		return s.compactSerializer
	}
	if _, ok := obj.(pubserialization.IdentifiedDataSerializable); ok {
		return s.identifiedSerializer
	}
	if _, ok := obj.(pubserialization.Portable); ok {
		return s.portableSerializer
	}
	return nil
}

func (s *Service) lookupBuiltinDeserializer(typeID int32) pubserialization.Serializer {
	switch typeID {
	case TypeNil:
		return nilSerializer
	case TypeCompact:
		return s.compactSerializer
	case TypePortable:
		return s.portableSerializer
	case TypeDataSerializable:
		return s.identifiedSerializer
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
		return javaBigIntegerSerializer
	case TypeJavaDecimal:
		return javaDecimalSerializer
	case TypeJSONSerialization:
		return jsonSerializer
	case TypeJavaArray:
		return javaArraySerializer
	case TypeJavaArrayList:
		return javaArrayListSerializer
	case TypeJavaLinkedList:
		return javaLinkedListSerializer
	case TypeJavaLocalDate:
		return javaLocalDateSerializer
	case TypeJavaLocalTime:
		return javaLocalTimeSerializer
	case TypeJavaLocalDateTime:
		return javaLocalDateTimeSerializer
	case TypeJavaOffsetDateTime:
		return javaOffsetDateTimeSerializer
	case TypeJavaClass:
		return javaClassSerializer
	case TypeGobSerialization:
		return gobSerializer
	}
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
		return ihzerrors.NewSerializationError("this serializer is already in the registry", nil)
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

func (s *Service) lookUpCustomSerializer(obj interface{}) pubserialization.Serializer {
	for key, val := range s.customSerializers {
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
	return s.SerializationConfig.GlobalSerializer()
}

func (s *Service) registerIdentifiedFactories() error {
	fs := map[int32]pubserialization.IdentifiedDataSerializableFactory{
		internal.AggregateFactoryID: &proxy.AggregateFactory{},
	}
	for _, f := range s.SerializationConfig.IdentifiedDataSerializableFactories() {
		fid := f.FactoryID()
		if _, ok := fs[fid]; ok {
			return ihzerrors.NewSerializationError("this serializer is already in the registry", nil)
		}
		fs[fid] = f
	}
	s.identifiedSerializer = NewIdentifiedDataSerializableSerializer(fs)
	if err := s.registerSerializer(s.identifiedSerializer); err != nil {
		return err
	}
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
	case int8:
		return int8Serializer
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
	case []int:
		return int64ArraySerializer
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
	case []interface{}:
		return javaArrayListSerializer
	case types.UUID:
		return uuidSerializer
	case types.LocalDate:
		return javaLocalDateSerializer
	case types.LocalTime:
		return javaLocalTimeSerializer
	case types.LocalDateTime:
		return javaLocalDateTimeSerializer
	case types.OffsetDateTime:
		return javaOffsetDateTimeSerializer
	case time.Time:
		return javaDateSerializer
	case *big.Int:
		return javaBigIntegerSerializer
	case types.Decimal:
		return javaDecimalSerializer
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
		return ihzerrors.NewSerializationError(v, nil)
	default:
		return fmt.Errorf("%v", rec)
	}
}

var nilSerializer = &NilSerializer{}
var boolSerializer = &BoolSerializer{}
var stringSerializer = &StringSerializer{}
var uint8Serializer = &ByteSerializer{}
var int8Serializer = &Int8Serializer{}
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
var javaBigIntegerSerializer = &JavaBigIntegerSerializer{}
var javaDecimalSerializer = &JavaDecimalSerializer{}
var javaClassSerializer = &JavaClassSerializer{}
var javaArraySerializer = &JavaArraySerializer{}
var javaArrayListSerializer = &JavaArrayListSerializer{}
var javaLinkedListSerializer = &JavaLinkedListSerializer{}
var javaLocalDateSerializer = &JavaLocalDateSerializer{}
var javaLocalTimeSerializer = &JavaLocalTimeSerializer{}
var javaLocalDateTimeSerializer = &JavaLocalDateTimeSerializer{}
var javaOffsetDateTimeSerializer = &JavaOffsetDateTimeSerializer{}
var gobSerializer = &GobSerializer{}
