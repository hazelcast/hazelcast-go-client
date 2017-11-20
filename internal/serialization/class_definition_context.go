// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
	. "github.com/hazelcast/go-client/internal/common"
	"reflect"
	"regexp"
	"strconv"
)

type ClassDefinitionContext struct {
	factoryId int32
	classDefs map[string]*ClassDefinition
}

func NewClassDefinitionContext(factoryId int32, portableVersion int32) *ClassDefinitionContext {
	return &ClassDefinitionContext{factoryId, make(map[string]*ClassDefinition)}
}

func (c *ClassDefinitionContext) LookUp(classId int32, version int32) *ClassDefinition {
	return c.classDefs[encodeVersionedClassId(classId, version)]
}

func (c *ClassDefinitionContext) Register(classDefinition *ClassDefinition) (*ClassDefinition, error) {
	if classDefinition == nil {
		return nil, nil
	}
	if classDefinition.factoryId != c.factoryId {
		return nil, NewHazelcastSerializationError(fmt.Sprintf("this factory's id is %d, intended factory id is %d.", c.factoryId, classDefinition.factoryId), nil)
	}
	classDefKey := encodeVersionedClassId(classDefinition.classId, classDefinition.factoryId)
	current := c.classDefs[classDefKey]
	if current == nil {
		c.classDefs[classDefKey] = classDefinition
		return classDefinition, nil
	}
	if !reflect.DeepEqual(current, classDefinition) {
		return nil, NewHazelcastSerializationError(fmt.Sprintf("incompatible class definition with same class id: %d", classDefinition.classId), nil)
	}
	return classDefinition, nil
}

func encodeVersionedClassId(classId int32, version int32) string {
	return strconv.Itoa(int(classId)) + "v" + strconv.Itoa(int(version))
}

func decodeVersionedClassId(encoded string) (int32, int32, error) {
	re := regexp.MustCompile("[0-9]+")
	pair := re.FindAllString(encoded, -1)
	classId, err := strconv.Atoi(pair[0])
	if err != nil {
		return 0, 0, err
	}
	version, err := strconv.Atoi(pair[1])
	if err != nil {
		return 0, 0, err
	}
	return int32(classId), int32(version), nil
}
