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

import "reflect"

// Config contains the serialization configuration of a Hazelcast instance.
type Config struct {
	GlobalSerializer                    Serializer
	IdentifiedDataSerializableFactories map[ // GlobalSerializer is the serializer that will be used if no other serializer is applicable.
	// IdentifiedDataSerializableFactories is a map of factory IDs and corresponding IdentifiedDataSerializable factories.
	int32]IdentifiedDataSerializableFactory
	PortableFactories map[ // PortableFactories is a map of factory IDs and corresponding Portable factories.
	int32]PortableFactory
	CustomSerializers map[ // CustomSerializers is a map of object types and corresponding custom serializers.
	reflect.Type]Serializer
	ClassDefinitions []ClassDefinition // ClassDefinitions contains ClassDefinitions for portable structs.

	PortableVersion int32
	BigEndian       bool // PortableVersion will be used to differentiate two versions of the same struct that have changes on the struct,
	// like adding/removing a field or changing a type of a field.
	// BigEndian is the Little Endinan byte order bool. If false, it is Big Endian.
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
