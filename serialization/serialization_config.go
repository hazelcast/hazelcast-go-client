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
	// globalSerializer is the serializer that will be used if no other serializer is applicable.
	globalSerializer Serializer
	// customSerializers is a map of object types and corresponding custom serializers.
	customSerializers map[reflect.Type]Serializer
	// identifiedDataSerializableFactories is a map of factory IDs and corresponding IdentifiedDataSerializable factories.
	identifiedDataSerializableFactories []IdentifiedDataSerializableFactory
	// portableFactories is a map of factory IDs and corresponding Portable factories.
	portableFactories []PortableFactory
	// classDefinitions contains classDefinitions for portable structs.
	classDefinitions []*ClassDefinition `json:"-"`
	PortableVersion  int32
	// PortableVersion will be used to differentiate two versions of the same struct that have changes on the struct,
	// like adding/removing a field or changing a type of a field.
	// LittleEndian sets byte order to Little Endian. Default is false.
	LittleEndian bool
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Clone() Config {
	idFactories := make([]IdentifiedDataSerializableFactory, len(c.identifiedDataSerializableFactories))
	copy(idFactories, c.identifiedDataSerializableFactories)
	pFactories := make([]PortableFactory, len(c.portableFactories))
	copy(pFactories, c.portableFactories)
	defs := make([]*ClassDefinition, len(c.classDefinitions))
	copy(defs, c.classDefinitions)
	serializers := map[reflect.Type]Serializer{}
	for k, v := range c.customSerializers {
		serializers[k] = v
	}
	return Config{
		LittleEndian:                        c.LittleEndian,
		identifiedDataSerializableFactories: idFactories,
		portableFactories:                   pFactories,
		PortableVersion:                     c.PortableVersion,
		customSerializers:                   serializers,
		globalSerializer:                    c.globalSerializer,
		classDefinitions:                    defs,
	}
}

func (c *Config) Validate() error {
	return nil
}

// AddIdentifiedDataSerializableFactory adds an identified data serializable factory.
func (b *Config) AddIdentifiedDataSerializableFactory(factory IdentifiedDataSerializableFactory) {
	b.identifiedDataSerializableFactories = append(b.identifiedDataSerializableFactories, factory)
}

// IdentifiedDataSerializableFactories returns a copy of identified data serializable factories.
func (b *Config) IdentifiedDataSerializableFactories() []IdentifiedDataSerializableFactory {
	fs := make([]IdentifiedDataSerializableFactory, len(b.identifiedDataSerializableFactories))
	copy(fs, b.identifiedDataSerializableFactories)
	return fs
}

// AddPortableFactory adds a portable factory.
func (b *Config) AddPortableFactory(factory PortableFactory) {
	b.portableFactories = append(b.portableFactories, factory)
}

// PortableFactories returns a copy of portable factories
func (b *Config) PortableFactories() []PortableFactory {
	fs := make([]PortableFactory, len(b.portableFactories))
	copy(fs, b.portableFactories)
	return fs
}

// AddCustomSerializer adds a customer serializer for the given type.
func (b *Config) AddCustomSerializer(t reflect.Type, serializer Serializer) error {
	if b.customSerializers == nil {
		b.customSerializers = map[reflect.Type]Serializer{}
	}
	if serializer.ID() <= 0 {
		return errors.New("serializerID must be positive")
	}
	b.customSerializers[t] = serializer
	return nil
}

// CustomSerializers returns a copy of custom serializers.
func (b *Config) CustomSerializers() map[reflect.Type]Serializer {
	sers := map[reflect.Type]Serializer{}
	if b.customSerializers != nil {
		for k, v := range b.customSerializers {
			sers[k] = v
		}
	}
	return sers
}

func (b *Config) AddClassDefinition(definition *ClassDefinition) {
	b.classDefinitions = append(b.classDefinitions, definition)
}

// ClassDefinitions returns a copy of class definitions.
func (b *Config) ClassDefinitions() []*ClassDefinition {
	cds := make([]*ClassDefinition, len(b.classDefinitions))
	copy(cds, b.classDefinitions)
	return cds
}

func (b *Config) SetGlobalSerializer(serializer Serializer) {
	b.globalSerializer = serializer
}

// GlobalSerializer returns the global serializer.
func (b *Config) GlobalSerializer() Serializer {
	return b.globalSerializer
}
