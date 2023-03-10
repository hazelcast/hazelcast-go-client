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
	"sync"

	pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

type SchemaMsg struct {
	ID         int64
	ResponseCh chan *Schema
}

type SchemaService struct {
	schemaMap map[int64]*Schema
	mu        *sync.RWMutex
	ch        chan<- SchemaMsg
}

func NewSchemaService(cfg pubserialization.CompactConfig, ch chan<- SchemaMsg) (*SchemaService, error) {
	sm, err := MakeSchemasFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &SchemaService{
		schemaMap: sm,
		mu:        &sync.RWMutex{},
		ch:        ch,
	}, nil
}

func (s *SchemaService) Schemas() []*Schema {
	s.mu.RLock()
	schemas := make([]*Schema, 0, len(s.schemaMap))
	for _, v := range s.schemaMap {
		schemas = append(schemas, v)
	}
	s.mu.RUnlock()
	return schemas
}

func (s *SchemaService) Get(ctx context.Context, schemaId int64) (schema *Schema, ok bool) {
	// TODO: return error
	s.mu.RLock()
	schema, ok = s.schemaMap[schemaId]
	s.mu.RUnlock()
	if !ok {
		rch := make(chan *Schema)
		s.ch <- SchemaMsg{ID: schemaId, ResponseCh: rch}
		select {
		case schema = <-rch:
			ok = schema != nil
			if ok {
				s.putLocal(schema)
			}
		case <-ctx.Done():
			if ctx.Err() != nil {
				return nil, false
			}
		}
	}
	return schema, ok
}

func (s *SchemaService) putLocal(schema *Schema) {
	s.mu.Lock()
	if _, ok := s.schemaMap[schema.ID()]; !ok {
		s.schemaMap[schema.ID()] = schema
	}
	s.mu.Unlock()
}
