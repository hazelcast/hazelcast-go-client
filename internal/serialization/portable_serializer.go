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
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
)

type PortableSerializer struct {
	service         *ServiceImpl
	portableContext *PortableContext
	factories       map[int32]PortableFactory
}

func NewPortableSerializer(service *ServiceImpl, portableFactories map[int32]PortableFactory,
	portableVersion int32) *PortableSerializer {
	return &PortableSerializer{service, NewPortableContext(service, portableVersion), portableFactories}
}

func (ps *PortableSerializer) ID() int32 {
	return ConstantTypePortable
}

func (ps *PortableSerializer) Read(input DataInput) (interface{}, error) {
	factoryID := input.ReadInt32()
	classID := input.ReadInt32()
	if input.Error() != nil {
		return nil, input.Error()
	}
	return ps.ReadObject(input, factoryID, classID)
}

func (ps *PortableSerializer) ReadObject(input DataInput, factoryID int32, classID int32) (Portable, error) {
	version := input.ReadInt32()
	if input.Error() != nil {
		return nil, input.Error()
	}
	portable, err := ps.createNewPortableInstance(factoryID, classID)
	if err != nil {
		return nil, err
	}

	classDefinition := ps.portableContext.LookUpClassDefinition(factoryID, classID, version)
	if classDefinition == nil {
		var backupPos = input.Position()
		classDefinition, err = ps.portableContext.ReadClassDefinitionFromInput(input, factoryID, classID, version)
		if err != nil {
			input.SetPosition(backupPos)
			return nil, err
		}
		input.SetPosition(backupPos)
	}
	var reader PortableReader
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

func (ps *PortableSerializer) createNewPortableInstance(factoryID int32, classID int32) (Portable, error) {
	factory := ps.factories[factoryID]
	if factory == nil {
		return nil, core.NewHazelcastSerializationError(fmt.Sprintf("there is no suitable portable factory for factory id: %d",
			factoryID), nil)
	}

	portable := factory.Create(classID)
	if portable == nil {
		return nil, core.NewHazelcastSerializationError(fmt.Sprintf("%v is not able to create an instance for id: %d on factory id: %d",
			reflect.TypeOf(factory), classID, factoryID), nil)
	}
	return portable, nil
}

func (ps *PortableSerializer) Write(output DataOutput, i interface{}) error {
	output.WriteInt32(i.(Portable).FactoryID())
	output.WriteInt32(i.(Portable).ClassID())
	err := ps.WriteObject(output, i)
	return err
}

func (ps *PortableSerializer) WriteObject(output DataOutput, i interface{}) error {
	classDefinition, err := ps.portableContext.LookUpOrRegisterClassDefiniton(i.(Portable))
	if err != nil {
		return err
	}
	output.WriteInt32(classDefinition.Version())
	writer := NewDefaultPortableWriter(ps, output.(PositionalDataOutput), classDefinition)
	err = i.(Portable).WritePortable(writer)
	if err != nil {
		return err
	}
	writer.End()
	return nil
}
