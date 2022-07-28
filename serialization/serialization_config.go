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
	"reflect"

	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

// Config contains the serialization configuration of a Hazelcast instance.
type Config struct {
	globalSerializer                    Serializer
	customSerializers                   map[reflect.Type]Serializer
	identifiedDataSerializableFactories []IdentifiedDataSerializableFactory
	portableFactories                   []PortableFactory
	classDefinitions                    []*ClassDefinition
	// PortableVersion will be used to differentiate two versions of the same struct that have changes on the struct,
	// like adding/removing a field or changing a type of a field.
	PortableVersion int32 `json:",omitempty"`
	// LittleEndian sets byte order to Little Endian. Default is false.
	LittleEndian bool `json:",omitempty"`
}

func (c *Config) Clone() Config {
	c.ensureCustomSerializers()
	var idFactories []IdentifiedDataSerializableFactory
	var pFactories []PortableFactory
	var defs []*ClassDefinition
	var serializers map[reflect.Type]Serializer
	if c.identifiedDataSerializableFactories != nil {
		// this is only necessary to make the cloned value exactly the same when c.identifiedDataSerializableFactories == nil
		idFactories = make([]IdentifiedDataSerializableFactory, len(c.identifiedDataSerializableFactories))
		copy(idFactories, c.identifiedDataSerializableFactories)
	}
	if c.portableFactories != nil {
		// this is only necessary to make the cloned value exactly the same when c.portableFactories == nil
		pFactories = make([]PortableFactory, len(c.portableFactories))
		copy(pFactories, c.portableFactories)
	}
	if c.classDefinitions != nil {
		// this is only necessary to make the cloned value exactly the same when c.classDefinitions == nil
		defs = make([]*ClassDefinition, len(c.classDefinitions))
		copy(defs, c.classDefinitions)
	}
	if c.customSerializers != nil {
		serializers = map[reflect.Type]Serializer{}
		for k, v := range c.customSerializers {
			serializers[k] = v
		}
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
	if c.customSerializers == nil {
		c.customSerializers = map[reflect.Type]Serializer{}
	}
	return nil
}

// SetIdentifiedDataSerializableFactories adds zore or more identified data serializable factories.
// Identified data serializable factories is a map of factory IDs and corresponding IdentifiedDataSerializable factories.
func (b *Config) SetIdentifiedDataSerializableFactories(factories ...IdentifiedDataSerializableFactory) {
	b.identifiedDataSerializableFactories = append(b.identifiedDataSerializableFactories, factories...)
}

// IdentifiedDataSerializableFactories returns a copy of identified data serializable factories.
// Identified data serializable factories is a map of factory IDs and corresponding IdentifiedDataSerializable factories.
func (b *Config) IdentifiedDataSerializableFactories() []IdentifiedDataSerializableFactory {
	fs := make([]IdentifiedDataSerializableFactory, len(b.identifiedDataSerializableFactories))
	copy(fs, b.identifiedDataSerializableFactories)
	return fs
}

// SetPortableFactories adds zero or more portable factories.
// Portable factories is a map of factory IDs and corresponding Portable factories.
func (b *Config) SetPortableFactories(factories ...PortableFactory) {
	b.portableFactories = append(b.portableFactories, factories...)
}

// PortableFactories returns a copy of portable factories.
// Portable factories is a map of factory IDs and corresponding Portable factories.
func (b *Config) PortableFactories() []PortableFactory {
	fs := make([]PortableFactory, len(b.portableFactories))
	copy(fs, b.portableFactories)
	return fs
}

// SetCustomSerializer adds a customer serializer for the given type.
// custom serializers is a map of object types and corresponding custom serializers.
func (b *Config) SetCustomSerializer(t reflect.Type, serializer Serializer) error {
	b.ensureCustomSerializers()
	if serializer.ID() <= 0 {
		return ihzerrors.NewIllegalArgumentError("serializerID must be positive", nil)
	}
	b.customSerializers[t] = serializer
	return nil
}

// CustomSerializers returns a copy of custom serializers.
// custom serializers is a map of object types and corresponding custom serializers.
func (b *Config) CustomSerializers() map[reflect.Type]Serializer {
	b.ensureCustomSerializers()
	sers := map[reflect.Type]Serializer{}
	for k, v := range b.customSerializers {
		sers[k] = v
	}
	return sers
}

// SetClassDefinitions adds zero or more class definitions for portable factories.
func (b *Config) SetClassDefinitions(definitions ...*ClassDefinition) {
	b.classDefinitions = append(b.classDefinitions, definitions...)
}

// ClassDefinitions returns a copy of class definitions.
// class definitions contains classDefinitions for portable structs.
func (b *Config) ClassDefinitions() []*ClassDefinition {
	cds := make([]*ClassDefinition, len(b.classDefinitions))
	copy(cds, b.classDefinitions)
	return cds
}

// SetGlobalSerializer sets the global serializer.
// Global serializer is the serializer that will be used if no other serializer is applicable.
func (b *Config) SetGlobalSerializer(serializer Serializer) {
	b.globalSerializer = serializer
}

// GlobalSerializer returns the global serializer.
// Global serializer is the serializer that will be used if no other serializer is applicable.
func (b *Config) GlobalSerializer() Serializer {
	return b.globalSerializer
}

func (b *Config) ensureCustomSerializers() {
	if b.customSerializers == nil {
		b.customSerializers = map[reflect.Type]Serializer{}
	}
}
