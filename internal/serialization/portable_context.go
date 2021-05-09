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

import (
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type PortableContext struct {
	service         *Service
	classDefContext map[int32]*ClassDefinitionContext
	portableVersion int32
}

func NewPortableContext(service *Service, portableVersion int32) *PortableContext {
	return &PortableContext{
		service:         service,
		portableVersion: portableVersion,
		classDefContext: make(map[int32]*ClassDefinitionContext),
	}
}

func (c *PortableContext) Version() int32 {
	return c.portableVersion
}

func (c *PortableContext) ReadClassDefinitionFromInput(input serialization.DataInput, factoryID int32, classID int32,
	version int32) serialization.ClassDefinition {
	var err error
	register := true
	classDefBuilder := NewClassDefinitionBuilder(factoryID, classID, version)
	input.ReadInt32()
	fieldCount := input.ReadInt32()
	offset := input.Position()
	for i := int32(0); i < fieldCount; i++ {
		pos := input.(*ObjectDataInput).ReadInt32AtPosition(offset + i*Int32SizeInBytes)
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
				c.ReadClassDefinitionFromInput(input, fieldFactoryID, fieldClassID, fieldVersion)
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
		if err = classDefBuilder.AddField(NewFieldDefinitionImpl(i, name, int32(fieldType),
			fieldFactoryID, fieldClassID, fieldVersion)); err != nil {
			panic(err)
		}
	}
	classDefinition := classDefBuilder.Build()
	if register {
		if classDefinition, err = c.RegisterClassDefinition(classDefinition); err != nil {
			panic(err)
		}
	}
	return classDefinition
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
		c.classDefContext[factoryID] = NewClassDefinitionContext(factoryID)
	}
	return c.classDefContext[factoryID].Register(classDefinition)
}

func (c *PortableContext) ClassVersion(portable serialization.Portable) int32 {
	if _, ok := portable.(serialization.VersionedPortable); ok {
		return portable.(serialization.VersionedPortable).Version()
	}
	return c.portableVersion
}
