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
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type PortableSerializer struct {
	service         *SerializationService
	portableContext *PortableContext
	factories       map[int32]serialization.PortableFactory
}

func NewPortableSerializer(service *SerializationService, portableFactories map[int32]serialization.PortableFactory, portableVersion int32) *PortableSerializer {
	return &PortableSerializer{service, NewPortableContext(service, portableVersion), portableFactories}
}

func (ps *PortableSerializer) Id() int32 {
	return ConstantTypePortable
}

func (ps *PortableSerializer) Read(input serialization.DataInput) (interface{}, error) {
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

func (ps *PortableSerializer) ReadObject(input serialization.DataInput, factoryId int32, classId int32) (serialization.Portable, error) {
	version, err := input.ReadInt32()
	if err != nil {
		return nil, err
	}

	factory := ps.factories[factoryId]
	if factory == nil {
		return nil, core.NewHazelcastSerializationError(fmt.Sprintf("there is no suitable portable factory for %v", factoryId), nil)
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
	var reader serialization.PortableReader
	var isMorphing bool
	if classDefinition.Version() == ps.portableContext.ClassVersion(portable) {
		reader = NewDefaultPortableReader(ps, input, classDefinition)
		isMorphing = false
	} else {
		reader = NewMorphingPortableReader(ps, input, classDefinition)
		isMorphing = true
	}

	err = portable.ReadPortable(reader)
	if err != nil {
		return nil, err
	}
	if isMorphing {
		reader.(*MorphingPortableReader).End()
	} else {
		reader.(*DefaultPortableReader).End()
	}

	return portable, nil
}

func (ps *PortableSerializer) Write(output serialization.DataOutput, i interface{}) error {
	output.WriteInt32(i.(serialization.Portable).FactoryId())
	output.WriteInt32(i.(serialization.Portable).ClassId())
	err := ps.WriteObject(output, i)
	return err
}

func (ps *PortableSerializer) WriteObject(output serialization.DataOutput, i interface{}) error {
	classDefinition, err := ps.portableContext.LookUpOrRegisterClassDefiniton(i.(serialization.Portable))
	if err != nil {
		return err
	}
	output.WriteInt32(classDefinition.Version())
	writer := NewDefaultPortableWriter(ps, output.(serialization.PositionalDataOutput), classDefinition)
	err = i.(serialization.Portable).WritePortable(writer)
	if err != nil {
		return err
	}
	writer.End()
	return nil
}
