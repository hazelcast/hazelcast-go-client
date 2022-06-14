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

type SchemaService struct {
	schemaMap map[int64]Schema
}

func NewSchemaService() SchemaService {
	return SchemaService{
		schemaMap: make(map[int64]Schema),
	}
}

func (s *SchemaService) Get(schemaId int64) (Schema, bool) {
	schema, ok := s.schemaMap[schemaId]
	return schema, ok
}

func (s *SchemaService) PutLocal(schema Schema) {
	s.schemaMap[schema.ID()] = schema
}
