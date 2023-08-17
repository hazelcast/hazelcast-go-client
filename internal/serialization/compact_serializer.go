/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
	"context"
	"fmt"
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

type GenericCompactDeserializer interface {
	Read(schema *Schema, reader pubserialization.CompactReader) interface{}
}

type CompactStreamSerializer struct {
	typeToSchema         map[reflect.Type]*Schema
	typeToSerializer     map[reflect.Type]pubserialization.CompactSerializer
	typeNameToSerializer map[string]pubserialization.CompactSerializer
	ss                   *SchemaService
	defaultDeserializer  GenericCompactDeserializer
}

func NewCompactStreamSerializer(cfg pubserialization.CompactConfig, schemaCh chan SchemaMsg, dds GenericCompactDeserializer) (*CompactStreamSerializer, error) {
	typeToSerializer := make(map[reflect.Type]pubserialization.CompactSerializer)
	typeNameToSerializer := make(map[string]pubserialization.CompactSerializer)
	typeToSchema := map[reflect.Type]*Schema{}
	for typeName, ser := range cfg.Serializers() {
		typeNameToSerializer[typeName] = ser
		typeToSerializer[ser.Type()] = ser
		s, err := makeSchemaFromSerializer(ser)
		if err != nil {
			return nil, err
		}
		typeToSchema[ser.Type()] = s
	}
	ss, err := NewSchemaService(cfg, schemaCh)
	if err != nil {
		return nil, err
	}
	return &CompactStreamSerializer{
		ss:                   ss,
		typeToSchema:         typeToSchema,
		typeToSerializer:     typeToSerializer,
		typeNameToSerializer: typeNameToSerializer,
		defaultDeserializer:  dds,
	}, nil
}

func (CompactStreamSerializer) ID() int32 {
	return TypeCompact
}

func (c CompactStreamSerializer) Read(input pubserialization.DataInput) interface{} {
	// TODO: move context to the method signature
	ctx := context.Background()
	schema := c.getOrReadSchema(ctx, input)
	serializer, ok := c.typeNameToSerializer[schema.TypeName]
	if !ok {
		if c.defaultDeserializer != nil {
			reader := NewDefaultCompactReader(c, input.(*ObjectDataInput), schema)
			return c.defaultDeserializer.Read(schema, reader)
		}
		panic(fmt.Sprintf("no compact serializer found for type: %s", schema.TypeName))
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
	schema := c.typeToSchema[t]
	// schema will always be non-nil at this point
	output.WriteInt64(schema.ID())
	w := NewDefaultCompactWriter(c, output.(*PositionalObjectDataOutput), schema)
	serializer.Write(w, object)
	w.End()
}

func (c CompactStreamSerializer) IsRegisteredAsCompact(t reflect.Type) bool {
	_, ok := c.typeToSerializer[t]
	return ok
}

func (c CompactStreamSerializer) getOrReadSchema(ctx context.Context, input pubserialization.DataInput) *Schema {
	schemaId := input.ReadInt64()
	schema, ok := c.ss.Get(ctx, schemaId)
	if !ok {
		panic(hzerrors.NewSerializationError(fmt.Sprintf("the schema cannot be found with id: %d", schemaId), nil))
	}
	return schema
}

func MakeSchemasFromConfig(cfg pubserialization.CompactConfig) (map[int64]*Schema, error) {
	r := map[int64]*Schema{}
	for _, ser := range cfg.Serializers() {
		s, err := makeSchemaFromSerializer(ser)
		if err != nil {
			return nil, err
		}
		r[s.ID()] = s
	}
	return r, nil
}

func makeSchemaFromSerializer(ser pubserialization.CompactSerializer) (schema *Schema, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = makeError(rec)
		}
	}()
	sw := NewSchemaWriter(ser.TypeName())
	// create the zero value for the type in the serializer and build the schema
	st := ser.Type()
	var v interface{}
	if st.Kind() == reflect.Ptr {
		v = reflect.New(st.Elem()).Interface()
	} else {
		v = reflect.Zero(st).Interface()
	}
	ser.Write(sw, v)
	return sw.Build(), nil
}
