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

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type CompactStreamSerializer struct {
	typeToSchema         map[reflect.Type]Schema
	typeToSerializer     map[reflect.Type]serialization.CompactSerializer
	typeNameToSerializer map[string]serialization.CompactSerializer
	schemaService        SchemaService
	rabin                RabinFingerPrint
}

func NewCompactStreamSerializer(compactConfig serialization.CompactConfig) *CompactStreamSerializer {
	typeToSchema := make(map[reflect.Type]Schema)
	typeToSerializer := make(map[reflect.Type]serialization.CompactSerializer)
	typeNameToSerializer := make(map[string]serialization.CompactSerializer)
	serializers := compactConfig.Serializers()
	for typeName, serializer := range serializers {
		typeNameToSerializer[typeName] = serializer
		typeToSerializer[serializer.Type()] = serializer
	}
	rabin := NewRabinFingerPrint()
	rabin.Init()
	return &CompactStreamSerializer{
		schemaService:        *NewSchemaService(),
		typeToSchema:         typeToSchema,
		typeToSerializer:     typeToSerializer,
		typeNameToSerializer: typeNameToSerializer,
		rabin:                rabin,
	}
}

func (CompactStreamSerializer) ID() int32 {
	return TypeCompact
}

func (c *CompactStreamSerializer) Read(input serialization.DataInput) interface{} {
	schema := c.getOrReadSchema(input)
	typeName := schema.TypeName()
	serializer, ok := c.typeNameToSerializer[typeName]
	if !ok {
		panic("No compact serializer found for type: " + typeName)
	}
	reader := NewDefaultCompactReader(*c, input.(*ObjectDataInput), schema)
	return serializer.Read(reader)
}

func (c *CompactStreamSerializer) Write(output serialization.DataOutput, object interface{}) {
	t := reflect.TypeOf(object)
	serializer := c.typeToSerializer[t]

	schema, ok := c.typeToSchema[t]
	if !ok {
		schemaWriter := NewSchemaWriter(serializer.TypeName())
		serializer.Write(schemaWriter, object)
		schema = schemaWriter.Build(c.rabin)
		c.schemaService.PutLocal(schema)
		c.typeToSchema[t] = schema
	}
	output.WriteInt64(schema.ID())
	writer := NewDefaultCompactWriter(*c, output.(*PositionalObjectDataOutput), schema)
	serializer.Write(writer, object)
	writer.End()
}

func (c *CompactStreamSerializer) IsRegisteredAsCompact(t reflect.Type) bool {
	_, ok := c.typeToSerializer[t]
	return ok
}

func (c *CompactStreamSerializer) getOrReadSchema(input serialization.DataInput) Schema {
	schemaId := input.ReadInt64()
	schema, ok := c.schemaService.Get(schemaId)
	if ok {
		return schema
	}
	panic(fmt.Sprintf("The schema cannot be found with id: %d", schemaId))
}
