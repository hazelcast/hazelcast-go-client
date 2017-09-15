package serialization

import (
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/serialization/api"
)

type PortableContext struct {
	service         *SerializationService
	portableVersion int32
	classDefContext map[int32]*ClassDefinitionContext
}

func NewPortableContext(service *SerializationService, portableVersion int32) *PortableContext {
	return &PortableContext{service, portableVersion, make(map[int32]*ClassDefinitionContext)}
}

func (c *PortableContext) GetVersion() int32 {
	return c.portableVersion
}

func (c *PortableContext) ReadClassDefinitionFromInput(input DataInput, factoryId int32, classId int32, version int32) *ClassDefinition {
	register := true
	classDefWriter := NewClassDefinitionWriter(c, factoryId, classId, version)
	input.ReadInt32()
	fieldCount, _ := input.ReadInt32()
	offset := input.GetPosition()
	for i := int32(0); i < fieldCount; i++ {
		pos, _ := input.(*ObjectDataInput).ReadInt32WithPosition(offset + i*INT_SIZE_IN_BYTES)
		input.SetPosition(pos)

		length, _ := input.ReadInt16()
		var temp []rune = make([]rune, length)
		for i := int16(0); i < length; i++ {
			char, _ := input.ReadByte()
			temp[i] = int32(char)
		}
		fieldType, _ := input.ReadByte()
		name := string(temp)
		var fieldFactoryId int32 = 0
		var fieldClassId int32 = 0
		if fieldType == PORTABLE {
			temp, _ := input.ReadBool()
			if temp {
				register = false
			}
			fieldFactoryId, _ := input.ReadInt32()
			fieldClassId, _ := input.ReadInt32()

			if register {
				fieldVersion, _ := input.ReadInt32()
				c.ReadClassDefinitionFromInput(input, fieldFactoryId, fieldClassId, fieldVersion)
			}
		} else if fieldType == PORTABLE_ARRAY {
			k, _ := input.ReadInt32()
			fieldFactoryId, _ := input.ReadInt32()
			fieldClassId, _ := input.ReadInt32()
			if k > 0 {
				p, _ := input.ReadInt32()
				input.SetPosition(p)
				fieldVersion, _ := input.ReadInt32()
				c.ReadClassDefinitionFromInput(input, fieldFactoryId, fieldClassId, fieldVersion)
			} else {
				register = false
			}
		}
		classDefWriter.addFieldByType(name, int32(fieldType), fieldFactoryId, fieldClassId)
	}
	classDefWriter.End()
	classDefinition := classDefWriter.GetDefinition()

	if register {
		classDefinition = classDefWriter.RegisterAndGet()
	}
	return classDefinition
}

func (c *PortableContext) LookUpOrRegisterClassDefiniton(portable Portable) *ClassDefinition {
	version := c.GetClassVersion(portable)
	classDef := c.LookUpClassDefinition(portable.GetFactoryId(), portable.GetClassId(), version)
	if classDef == nil {
		classDef = c.GenerateClassDefinitionForPortable(portable)
		c.RegisterClassDefinition(classDef)
	}
	return classDef

}

func (c *PortableContext) LookUpClassDefinition(factoryId int32, classId int32, version int32) *ClassDefinition {
	factory := c.classDefContext[factoryId]
	if factory == nil {
		return nil
	} else {
		return factory.LookUp(classId, version)
	}
}

func (c *PortableContext) GenerateClassDefinitionForPortable(portable Portable) *ClassDefinition {
	version := c.GetClassVersion(portable)
	classDefinitionWriter := NewClassDefinitionWriter(c, portable.GetFactoryId(), portable.GetClassId(), version)
	portable.WritePortable(classDefinitionWriter)
	classDefinitionWriter.End()
	return classDefinitionWriter.RegisterAndGet()
}

func (c *PortableContext) RegisterClassDefinition(classDefinition *ClassDefinition) *ClassDefinition {
	factoryId := classDefinition.factoryId
	if c.classDefContext[factoryId] == nil {
		c.classDefContext[factoryId] = NewClassDefinitionContext(factoryId, c.portableVersion)
	}
	return c.classDefContext[factoryId].Register(classDefinition)
}

func (c *PortableContext) GetClassVersion(portable Portable) int32 {
	//TODO should be controlled
	return c.portableVersion
}
