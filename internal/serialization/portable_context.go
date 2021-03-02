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
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/bufutil"
)

type PortableContext struct {
	service         *ServiceImpl
	portableVersion int32
	classDefContext map[int32]*ClassDefinitionContext
}

func NewPortableContext(service *ServiceImpl, portableVersion int32) *PortableContext {
	return &PortableContext{service, portableVersion, make(map[int32]*ClassDefinitionContext)}
}

func (c *PortableContext) Version() int32 {
	return c.portableVersion
}

func (c *PortableContext) ReadClassDefinitionFromInput(input DataInput, factoryID int32, classID int32,
	version int32) (ClassDefinition, error) {
	var err error
	register := true
	classDefBuilder := NewClassDefinitionBuilder(factoryID, classID, version)
	input.ReadInt32()
	fieldCount := input.ReadInt32()
	offset := input.Position()
	for i := int32(0); i < fieldCount; i++ {
		pos := input.(*ObjectDataInput).ReadInt32WithPosition(offset + i*bufutil.Int32SizeInBytes)
		input.SetPosition(pos)

		length := input.ReadInt16()
		var temp = make([]rune, length)
		for i := int16(0); i < length; i++ {
			char := input.ReadByte()
			temp[i] = int32(char)
		}
		fieldType := input.ReadByte()
		name := string(temp)
		var fieldFactoryID int32
		var fieldClassID int32
		fieldVersion := version
		if fieldType == TypePortable {
			temp := input.ReadBool()
			if temp {
				register = false
			}
			fieldFactoryID = input.ReadInt32()
			fieldClassID = input.ReadInt32()
			if register {
				fieldVersion = input.ReadInt32()
				_, err := c.ReadClassDefinitionFromInput(input, fieldFactoryID, fieldClassID, fieldVersion)
				if err != nil {
					return nil, err
				}
			}
		} else if fieldType == TypePortableArray {
			k := input.ReadInt32()
			fieldFactoryID = input.ReadInt32()
			fieldClassID = input.ReadInt32()
			if k > 0 {
				p := input.ReadInt32()
				input.SetPosition(p)
				fieldVersion = input.ReadInt32()
				// XXX: Output and error are not handled
				c.ReadClassDefinitionFromInput(input, fieldFactoryID, fieldClassID, fieldVersion)
			} else {
				register = false
			}
		}
		if input.Error() != nil {
			return nil, input.Error()
		}
		err = classDefBuilder.AddField(NewFieldDefinitionImpl(i, name, int32(fieldType),
			fieldFactoryID, fieldClassID, fieldVersion))
		if err != nil {
			return nil, err
		}
	}
	if input.Error() != nil {
		return nil, input.Error()
	}
	classDefinition := classDefBuilder.Build()

	if register {
		var err error
		classDefinition, err = c.RegisterClassDefinition(classDefinition)
		if err != nil {
			return nil, err
		}
	}
	return classDefinition, nil
}

func (c *PortableContext) LookUpOrRegisterClassDefiniton(portable Portable) (ClassDefinition, error) {
	var err error
	version := c.ClassVersion(portable)
	classDef := c.LookUpClassDefinition(portable.FactoryID(), portable.ClassID(), version)
	if classDef == nil {
		writer := NewClassDefinitionWriter(c, portable.FactoryID(), portable.ClassID(), version)
		err = portable.WritePortable(writer)
		if err != nil {
			return nil, err
		}
		classDef, err = writer.registerAndGet()
	}
	return classDef, err

}

func (c *PortableContext) LookUpClassDefinition(factoryID int32, classID int32, version int32) ClassDefinition {
	factory := c.classDefContext[factoryID]
	if factory == nil {
		return nil
	}
	return factory.LookUp(classID, version)
}

func (c *PortableContext) RegisterClassDefinition(classDefinition ClassDefinition) (
	ClassDefinition, error) {
	factoryID := classDefinition.FactoryID()
	if c.classDefContext[factoryID] == nil {
		c.classDefContext[factoryID] = NewClassDefinitionContext(factoryID)
	}
	return c.classDefContext[factoryID].Register(classDefinition)
}

func (c *PortableContext) ClassVersion(portable Portable) int32 {
	if _, ok := portable.(VersionedPortable); ok {
		return portable.(VersionedPortable).Version()
	}
	return c.portableVersion
}
