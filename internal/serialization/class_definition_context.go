// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serialization

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hzerror"
	"reflect"
	"strconv"
)

type ClassDefinitionContext struct {
	factoryID int32
	classDefs map[string]ClassDefinition
}

func NewClassDefinitionContext(factoryID int32) *ClassDefinitionContext {
	return &ClassDefinitionContext{factoryID, make(map[string]ClassDefinition)}
}

func (c *ClassDefinitionContext) LookUp(classID int32, version int32) ClassDefinition {
	return c.classDefs[encodeVersionedClassID(classID, version)]
}

func (c *ClassDefinitionContext) Register(classDefinition ClassDefinition) (ClassDefinition, error) {
	if classDefinition == nil {
		return nil, nil
	}
	if classDefinition.FactoryID() != c.factoryID {
		return nil, hzerror.NewHazelcastSerializationError(fmt.Sprintf("this factory's id is %d, intended factory id is %d.",
			c.factoryID, classDefinition.FactoryID()), nil)
	}
	classDefKey := encodeVersionedClassID(classDefinition.ClassID(), classDefinition.Version())
	current := c.classDefs[classDefKey]
	if current == nil {
		c.classDefs[classDefKey] = classDefinition
		return classDefinition, nil
	}
	if !reflect.DeepEqual(current, classDefinition) {
		return nil, hzerror.NewHazelcastSerializationError(fmt.Sprintf("incompatible class definition with same class id: %d",
			classDefinition.ClassID()), nil)
	}
	return classDefinition, nil
}

func encodeVersionedClassID(classID int32, version int32) string {
	return strconv.Itoa(int(classID)) + "v" + strconv.Itoa(int(version))
}
