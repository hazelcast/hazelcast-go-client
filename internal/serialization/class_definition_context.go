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

package serialization

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"

	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type ClassDefinitionContext struct {
	classDefs map[string]*serialization.ClassDefinition
	mu        *sync.RWMutex
	factoryID int32
}

func NewClassDefinitionContext(factoryID int32) *ClassDefinitionContext {
	return &ClassDefinitionContext{
		factoryID: factoryID,
		classDefs: make(map[string]*serialization.ClassDefinition),
		mu:        &sync.RWMutex{},
	}
}

func (c *ClassDefinitionContext) LookUp(classID int32, version int32) *serialization.ClassDefinition {
	c.mu.RLock()
	cd := c.classDefs[encodeVersionedClassID(classID, version)]
	c.mu.RUnlock()
	return cd
}

func (c *ClassDefinitionContext) register(classDefinition *serialization.ClassDefinition) error {
	if classDefinition == nil {
		return nil
	}
	if classDefinition.FactoryID != c.factoryID {
		text := fmt.Sprintf("this factory's id is %d, intended factory id is %d.", c.factoryID, classDefinition.FactoryID)
		return ihzerrors.NewSerializationError(text, nil)
	}
	k := encodeVersionedClassID(classDefinition.ClassID, classDefinition.Version)
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.classDefs[k]
	if current == nil {
		c.classDefs[k] = classDefinition
		return nil
	}
	if !reflect.DeepEqual(current, classDefinition) {
		return ihzerrors.NewSerializationError(fmt.Sprintf("incompatible class definition with same class id: %d",
			classDefinition.ClassID), nil)
	}
	return nil
}

func encodeVersionedClassID(classID int32, version int32) string {
	return strconv.Itoa(int(classID)) + "v" + strconv.Itoa(int(version))
}
