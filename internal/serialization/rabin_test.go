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

package serialization_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	pserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

func TestRabinFingerprintIsConsistentWithWrittenData(t *testing.T) {
	rabin := serialization.NewRabinFingerPrint()
	fieldDefinitionMap := make(map[string]*serialization.FieldDescriptor)
	ageField := serialization.NewFieldDescriptor("age", pserialization.FieldKindInt32)
	nameField := serialization.NewFieldDescriptor("name", pserialization.FieldKindString)
	fieldDefinitionMap["age"] = &ageField
	fieldDefinitionMap["name"] = &nameField

	schema := serialization.NewSchema("student", fieldDefinitionMap, rabin)
	schemaId := schema.ID()
	/*
		The magic number is generated using the following code snippet:

		SchemaWriter writer = new SchemaWriter("student");
		writer.addField(new FieldDescriptor("age", FieldKind.INT32));
		writer.addField(new FieldDescriptor("name", FieldKind.STRING));
		Schema schema = writer.build();
		System.out.println(schema.getSchemaId());
	*/
	assert.Equal(t, int64(5500194539746463554), schemaId)
}
