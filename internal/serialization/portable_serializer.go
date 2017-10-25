package serialization

import (
	"fmt"
	. "github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/serialization/api"
)

type PortableSerializer struct {
	service         *SerializationService
	portableContext *PortableContext
	factories       map[int32]PortableFactory
}

func NewPortableSerializer(service *SerializationService, portableFactories map[int32]PortableFactory, portableVersion int32) *PortableSerializer {
	return &PortableSerializer{service, NewPortableContext(service, portableVersion), portableFactories}
}

func (ps *PortableSerializer) Id() int32 {
	return CONSTANT_TYPE_PORTABLE
}

func (ps *PortableSerializer) Read(input DataInput) (interface{}, error) {
	factoryId, err := input.ReadInt32()
	if err != nil {
		return nil, err
	}
	classId, err := input.ReadInt32()
	if err != nil {
		return nil, err
	}
	return ps.ReadObject(input, factoryId, classId)
}

func (ps *PortableSerializer) ReadObject(input DataInput, factoryId int32, classId int32) (Portable, error) {
	version, err := input.ReadInt32()
	if err != nil {
		return nil, err
	}

	factory := ps.factories[factoryId]
	if factory == nil {
		return nil, NewHazelcastSerializationError(fmt.Sprintf("there is no suitable portable factory for %v", factoryId), nil)
	}

	portable := factory.Create(classId)
	classDefinition := ps.portableContext.LookUpClassDefinition(factoryId, classId, version)
	if classDefinition == nil {
		var backupPos = input.Position()
		classDefinition, err = ps.portableContext.ReadClassDefinitionFromInput(input, factoryId, classId, version)
		if err != nil {
			input.SetPosition(backupPos)
			return nil, err
		}
		input.SetPosition(backupPos)
	}
	var reader PortableReader
	if classDefinition.version == ps.portableContext.ClassVersion(portable) {
		reader = NewDefaultPortableReader(ps, input, classDefinition)
	} else {
		reader = NewMorphingPortableReader(ps, input, classDefinition)
	}
	portable.ReadPortable(reader)
	reader.End()
	return portable, nil
}

func (ps *PortableSerializer) Write(output DataOutput, i interface{}) {
	output.WriteInt32(i.(Portable).FactoryId())
	output.WriteInt32(i.(Portable).ClassId())
	ps.WriteObject(output, i)
}

func (ps *PortableSerializer) WriteObject(output DataOutput, i interface{}) {
	classDefinition, _ := ps.portableContext.LookUpOrRegisterClassDefiniton(i.(Portable))
	output.WriteInt32(classDefinition.version)
	writer := NewDefaultPortableWriter(ps, output.(PositionalDataOutput), classDefinition)
	i.(Portable).WritePortable(writer)
	writer.End()
}
