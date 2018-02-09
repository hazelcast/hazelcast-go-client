// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
	. "github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization/classdef"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/classdef"
)

type PortableContext struct {
	service         *SerializationService
	portableVersion int32
	classDefContext map[int32]*ClassDefinitionContext
}

func NewPortableContext(service *SerializationService, portableVersion int32) *PortableContext {
	return &PortableContext{service, portableVersion, make(map[int32]*ClassDefinitionContext)}
}

func (c *PortableContext) Version() int32 {
	return c.portableVersion
}

func (c *PortableContext) ReadClassDefinitionFromInput(input DataInput, factoryId int32, classId int32, version int32) (ClassDefinition, error) {
	register := true
	classDefBuilder := classdef.NewClassDefinitionBuilder(factoryId, classId, version)
	input.ReadInt32()
	fieldCount, err := input.ReadInt32()
	if err != nil {
		return nil, err
	}
	offset := input.Position()
	for i := int32(0); i < fieldCount; i++ {
		pos, err := input.(*ObjectDataInput).ReadInt32WithPosition(offset + i*INT_SIZE_IN_BYTES)
		if err != nil {
			return nil, err
		}
		input.SetPosition(pos)

		length, err := input.ReadInt16()
		if err != nil {
			return nil, err
		}
		var temp []rune = make([]rune, length)
		for i := int16(0); i < length; i++ {
			char, err := input.ReadByte()
			if err != nil {
				return nil, err
			}
			temp[i] = int32(char)
		}
		fieldType, err := input.ReadByte()
		if err != nil {
			return nil, err
		}
		name := string(temp)
		var fieldFactoryId int32 = 0
		var fieldClassId int32 = 0
		var fieldVersion int32 = 0
		if fieldType == PORTABLE {
			temp, err := input.ReadBool()
			if err != nil {
				return nil, err
			}
			if temp {
				register = false
			}
			fieldFactoryId, err = input.ReadInt32()
			if err != nil {
				return nil, err
			}
			fieldClassId, err = input.ReadInt32()
			if err != nil {
				return nil, err
			}

			if register {
				fieldVersion, err = input.ReadInt32()
				if err != nil {
					return nil, err
				}
				c.ReadClassDefinitionFromInput(input, fieldFactoryId, fieldClassId, fieldVersion)
			}
		} else if fieldType == PORTABLE_ARRAY {
			k, err := input.ReadInt32()
			if err != nil {
				return nil, err
			}
			fieldFactoryId, err = input.ReadInt32()
			if err != nil {
				return nil, err
			}
			fieldClassId, err = input.ReadInt32()
			if err != nil {
				return nil, err
			}
			if k > 0 {
				p, err := input.ReadInt32()
				if err != nil {
					return nil, err
				}
				input.SetPosition(p)
				fieldVersion, err = input.ReadInt32()
				if err != nil {
					return nil, err
				}
				c.ReadClassDefinitionFromInput(input, fieldFactoryId, fieldClassId, fieldVersion)
			} else {
				register = false
			}
		}
		classDefBuilder.AddField(NewFieldDefinitionImpl(i, name, int32(fieldType), fieldFactoryId, fieldClassId, fieldVersion))
	}

	classDefinition := classDefBuilder.Build()

	if register {
		classDefinition, err = c.RegisterClassDefinition(classDefinition)
		if err != nil {
			return classDefinition, nil
		}
	}
	return classDefinition, nil
}

func (c *PortableContext) LookUpOrRegisterClassDefiniton(portable Portable) (ClassDefinition, error) {
	var err error
	version := c.ClassVersion(portable)
	classDef := c.LookUpClassDefinition(portable.FactoryId(), portable.ClassId(), version)
	if classDef == nil {
		writer := NewClassDefinitionWriter(c, portable.FactoryId(), portable.ClassId(), version)
		portable.WritePortable(writer)
		classDef, err = writer.registerAndGet()
	}
	return classDef, err

}

func (c *PortableContext) LookUpClassDefinition(factoryId int32, classId int32, version int32) ClassDefinition {
	factory := c.classDefContext[factoryId]
	if factory == nil {
		return nil
	} else {
		return factory.LookUp(classId, version)
	}
}

func (c *PortableContext) RegisterClassDefinition(classDefinition ClassDefinition) (ClassDefinition, error) {
	factoryId := classDefinition.FactoryId()
	if c.classDefContext[factoryId] == nil {
		c.classDefContext[factoryId] = NewClassDefinitionContext(factoryId, c.portableVersion)
	}
	return c.classDefContext[factoryId].Register(classDefinition)
}

func (c *PortableContext) ClassVersion(portable Portable) int32 {
	if _, ok := portable.(VersionedPortable); ok {
		return portable.(VersionedPortable).Version()
	}
	return c.portableVersion
}
