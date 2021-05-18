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
	"reflect"
)

// Config contains the serialization configuration of a Hazelcast instance.
type Config struct {
	// BigEndian is the Little Endinan byte order bool. If false, it is Big Endian.
	BigEndian bool
	// PortableFactories is a map of factory IDs and corresponding Portable factories.
	PortableFactories map[int32]PortableFactory
	// PortableVersion will be used to differentiate two versions of the same struct that have changes on the struct,
	// like adding/removing a field or changing a type of a field.
	PortableVersion int32
	// CustomSerializers is a map of object types and corresponding custom serializers.
	CustomSerializers map[reflect.Type]Serializer
	// GlobalSerializer is the serializer that will be used if no other serializer is applicable.
	GlobalSerializer Serializer
	// ClassDefinitions contains ClassDefinitions for portable structs.
	ClassDefinitions []ClassDefinition
	// IdentifiedDataSerializableFactories is a map of factory IDs and corresponding IdentifiedDataSerializable factories.
	IdentifiedDataSerializableFactories map[int32]IdentifiedDataSerializableFactory
}

func (c Config) Clone() Config {
	idFactories := map[int32]IdentifiedDataSerializableFactory{}
	for k, v := range c.IdentifiedDataSerializableFactories {
		idFactories[k] = v
	}
	pFactories := map[int32]PortableFactory{}
	for k, v := range c.PortableFactories {
		pFactories[k] = v
	}
	serializers := map[reflect.Type]Serializer{}
	for k, v := range c.CustomSerializers {
		serializers[k] = v
	}
	defs := make([]ClassDefinition, len(c.ClassDefinitions))
	copy(defs, c.ClassDefinitions)
	return Config{
		BigEndian:                           c.BigEndian,
		IdentifiedDataSerializableFactories: idFactories,
		PortableFactories:                   pFactories,
		PortableVersion:                     c.PortableVersion,
		CustomSerializers:                   serializers,
		GlobalSerializer:                    c.GlobalSerializer,
		ClassDefinitions:                    defs,
	}
}

// AddIdentifiedDataSerializableFactory adds an identified data serializable factory.
func (b *Config) AddIdentifiedDataSerializableFactory(factory IdentifiedDataSerializableFactory) {
	b.IdentifiedDataSerializableFactories[factory.FactoryID()] = factory
}

// AddPortableFactory adds a portable factory.
func (b *Config) AddPortableFactory(factory PortableFactory) {
	b.PortableFactories[factory.FactoryID()] = factory
}

// AddCustomSerializer adds a customer serializer for the given type.
func (b *Config) AddCustomSerializer(t reflect.Type, serializer Serializer) error {
	if serializer.ID() <= 0 {
		return errors.New("serializerID must be positive")
	}
	b.CustomSerializers[t] = serializer
	return nil
}

func (b *Config) AddClassDefinition(definition ClassDefinition) {
	b.ClassDefinitions = append(b.ClassDefinitions, definition)
}
