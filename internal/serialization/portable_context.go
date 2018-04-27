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
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	internalclassdef "github.com/hazelcast/hazelcast-go-client/internal/serialization/classdef"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/serialization/classdef"
)

type PortableContext struct {
	service         *Service
	portableVersion int32
	classDefContext map[int32]*internalclassdef.ClassDefinitionContext
}

func NewPortableContext(service *Service, portableVersion int32) *PortableContext {
	return &PortableContext{service, portableVersion, make(map[int32]*internalclassdef.ClassDefinitionContext)}
}

func (c *PortableContext) Version() int32 {
	return c.portableVersion
}

func (c *PortableContext) ReadClassDefinitionFromInput(input serialization.DataInput, factoryID int32, classID int32,
	version int32) (serialization.ClassDefinition, error) {
	register := true
	classDefBuilder := classdef.NewClassDefinitionBuilder(factoryID, classID, version)
	input.ReadInt32()
	fieldCount, err := input.ReadInt32()
	if err != nil {
		return nil, err
	}
	offset := input.Position()
	for i := int32(0); i < fieldCount; i++ {
		pos, err := input.(*ObjectDataInput).ReadInt32WithPosition(offset + i*common.Int32SizeInBytes)
		if err != nil {
			return nil, err
		}
		input.SetPosition(pos)

		length, err := input.ReadInt16()
		if err != nil {
			return nil, err
		}
		var temp = make([]rune, length)
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
		var fieldFactoryID int32
		var fieldClassID int32
		var fieldVersion int32
		if fieldType == internalclassdef.TypePortable {
			temp, err := input.ReadBool()
			if err != nil {
				return nil, err
			}
			if temp {
				register = false
			}
			fieldFactoryID, err = input.ReadInt32()
			if err != nil {
				return nil, err
			}
			fieldClassID, err = input.ReadInt32()
			if err != nil {
				return nil, err
			}

			if register {
				fieldVersion, err = input.ReadInt32()
				if err != nil {
					return nil, err
				}
				c.ReadClassDefinitionFromInput(input, fieldFactoryID, fieldClassID, fieldVersion)
			}
		} else if fieldType == internalclassdef.TypePortableArray {
			k, err := input.ReadInt32()
			if err != nil {
				return nil, err
			}
			fieldFactoryID, err = input.ReadInt32()
			if err != nil {
				return nil, err
			}
			fieldClassID, err = input.ReadInt32()
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
				c.ReadClassDefinitionFromInput(input, fieldFactoryID, fieldClassID, fieldVersion)
			} else {
				register = false
			}
		}
		classDefBuilder.AddField(internalclassdef.NewFieldDefinitionImpl(i, name, int32(fieldType),
			fieldFactoryID, fieldClassID, fieldVersion))
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

func (c *PortableContext) LookUpOrRegisterClassDefiniton(portable serialization.Portable) (serialization.ClassDefinition, error) {
	var err error
	version := c.ClassVersion(portable)
	classDef := c.LookUpClassDefinition(portable.FactoryID(), portable.ClassID(), version)
	if classDef == nil {
		writer := NewClassDefinitionWriter(c, portable.FactoryID(), portable.ClassID(), version)
		portable.WritePortable(writer)
		classDef, err = writer.registerAndGet()
	}
	return classDef, err

}

func (c *PortableContext) LookUpClassDefinition(factoryID int32, classID int32, version int32) serialization.ClassDefinition {
	factory := c.classDefContext[factoryID]
	if factory == nil {
		return nil
	}
	return factory.LookUp(classID, version)
}

func (c *PortableContext) RegisterClassDefinition(classDefinition serialization.ClassDefinition) (
	serialization.ClassDefinition, error) {
	factoryID := classDefinition.FactoryID()
	if c.classDefContext[factoryID] == nil {
		c.classDefContext[factoryID] = internalclassdef.NewClassDefinitionContext(factoryID)
	}
	return c.classDefContext[factoryID].Register(classDefinition)
}

func (c *PortableContext) ClassVersion(portable serialization.Portable) int32 {
	if _, ok := portable.(serialization.VersionedPortable); ok {
		return portable.(serialization.VersionedPortable).Version()
	}
	return c.portableVersion
}
