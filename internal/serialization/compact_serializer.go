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
	classToSchema    map[reflect.Type]Schema
	classToSerializer    map[reflect.Type]serialization.CompactSerializer
	typeNameToSerializer map[string]serialization.CompactSerializer
	schemaService SchemaService
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
	serializer, _ := c.classToSerializer[t]

	schema, ok := c.classToSchema[t]
	if !ok {

	}
	output.WriteInt64(schema.SchemaID())
	writer := NewDefaultCompactWriter(*c, output.(*PositionalObjectDataOutput), schema)
	serializer.Write(writer, object)
	writer.End()
}

func (c *CompactStreamSerializer) IsRegisteredAsCompact(t reflect.Type) bool {
	_, ok := c.classToSerializer[t]
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
