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

	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

type CompactStreamSerializer struct {
	typeToSchema         map[reflect.Type]Schema
	typeToSerializer     map[reflect.Type]pubserialization.CompactSerializer
	typeNameToSerializer map[string]pubserialization.CompactSerializer
	ss                   *SchemaService
	fingerprint          RabinFingerPrint
}

func NewCompactStreamSerializer(cfg pubserialization.CompactConfig) *CompactStreamSerializer {
	typeToSerializer := make(map[reflect.Type]pubserialization.CompactSerializer)
	typeNameToSerializer := make(map[string]pubserialization.CompactSerializer)
	for typeName, serializer := range cfg.Serializers() {
		typeNameToSerializer[typeName] = serializer
		typeToSerializer[serializer.Type()] = serializer
	}
	return &CompactStreamSerializer{
		ss:                   NewSchemaService(),
		typeToSchema:         make(map[reflect.Type]Schema),
		typeToSerializer:     typeToSerializer,
		typeNameToSerializer: typeNameToSerializer,
		fingerprint:          NewRabinFingerPrint(),
	}
}

func (CompactStreamSerializer) ID() int32 {
	return TypeCompact
}

func (c CompactStreamSerializer) Read(input pubserialization.DataInput) interface{} {
	schema := c.getOrReadSchema(input)
	typeName := schema.TypeName()
	serializer, ok := c.typeNameToSerializer[typeName]
	if !ok {
		panic(fmt.Sprintf("no compact serializer found for type: %s", typeName))
	}
	reader := NewDefaultCompactReader(c, input.(*ObjectDataInput), schema)
	return serializer.Read(reader)
}

func (c CompactStreamSerializer) Write(output pubserialization.DataOutput, object interface{}) {
	t := reflect.TypeOf(object)
	serializer, ok := c.typeToSerializer[t]
	if !ok {
		panic(fmt.Sprintf("no compact serializer found for type: %s", t.Name()))
	}
	schema, ok := c.typeToSchema[t]
	if !ok {
		sw := NewSchemaWriter(serializer.TypeName())
		serializer.Write(sw, object)
		schema = sw.Build(c.fingerprint)
		c.ss.PutLocal(schema)
		c.typeToSchema[t] = schema
	}
	output.WriteInt64(schema.ID())
	w := NewDefaultCompactWriter(c, output.(*PositionalObjectDataOutput), schema)
	serializer.Write(w, object)
	w.End()
}

func (c CompactStreamSerializer) IsRegisteredAsCompact(t reflect.Type) bool {
	_, ok := c.typeToSerializer[t]
	return ok
}

func (c CompactStreamSerializer) getOrReadSchema(input pubserialization.DataInput) Schema {
	schemaId := input.ReadInt64()
	schema, ok := c.ss.Get(schemaId)
	if !ok {
		panic(hzerrors.NewSerializationError(fmt.Sprintf("the schema cannot be found with id: %d", schemaId), nil))
	}
	return schema
}
