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
	"fmt"
	"reflect"

	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type PortableSerializer struct {
	service         *Service
	portableContext *PortableContext
	factories       map[int32]serialization.PortableFactory
}

func NewPortableSerializer(service *Service, factories []serialization.PortableFactory, version int32) (*PortableSerializer, error) {
	pf := map[int32]serialization.PortableFactory{}
	for _, f := range factories {
		fid := f.FactoryID()
		if _, ok := pf[fid]; ok {
			return nil, ihzerrors.NewSerializationError("this serializer is already in the registry", nil)
		}
		pf[fid] = f
	}
	ser := &PortableSerializer{service, NewPortableContext(service, version), pf}
	return ser, nil
}

func (ps *PortableSerializer) ID() int32 {
	return TypePortable
}

func (ps *PortableSerializer) Read(input serialization.DataInput) interface{} {
	factoryID := input.ReadInt32()
	classID := input.ReadInt32()
	return ps.ReadObject(input, factoryID, classID)
}

func (ps *PortableSerializer) ReadObject(input serialization.DataInput, factoryID int32, classID int32) serialization.Portable {
	version := input.ReadInt32()
	portable, err := ps.createNewPortableInstance(factoryID, classID)
	if err != nil {
		panic(err)
	}
	classDefinition := ps.portableContext.LookUpClassDefinition(factoryID, classID, version)
	if classDefinition == nil {
		var backupPos = input.Position()
		classDefinition = ps.portableContext.ReadClassDefinitionFromInput(input, factoryID, classID, version)
		input.SetPosition(backupPos)
	}
	var reader serialization.PortableReader
	var isMorphing bool
	if classDefinition.Version == ps.portableContext.ClassVersion(portable) {
		reader = NewDefaultPortableReader(ps, input, classDefinition)
		isMorphing = false
	} else {
		reader = NewMorphingPortableReader(ps, input, classDefinition)
		isMorphing = true
	}
	portable.ReadPortable(reader)
	if isMorphing {
		reader.(*MorphingPortableReader).End()
	} else {
		reader.(*DefaultPortableReader).End()
	}
	return portable
}

func (ps *PortableSerializer) createNewPortableInstance(factoryID int32, classID int32) (serialization.Portable, error) {
	factory := ps.factories[factoryID]
	if factory == nil {
		return nil, ihzerrors.NewSerializationError(fmt.Sprintf("there is no suitable portable factory for factory id: %d",
			factoryID), nil)
	}

	portable := factory.Create(classID)
	if portable == nil {
		return nil, ihzerrors.NewSerializationError(fmt.Sprintf("%v is not able to create an instance for id: %d on factory id: %d",
			reflect.TypeOf(factory), classID, factoryID), nil)
	}
	return portable, nil
}

func (ps *PortableSerializer) Write(output serialization.DataOutput, i interface{}) {
	p, ok := i.(serialization.Portable)
	if !ok {
		panic("not a portable")
	}
	output.WriteInt32(p.FactoryID())
	output.WriteInt32(p.ClassID())
	ps.WriteObject(output, i)
}

func (ps *PortableSerializer) WriteObject(output serialization.DataOutput, i interface{}) {
	classDefinition, err := ps.portableContext.LookUpOrRegisterClassDefiniton(i.(serialization.Portable))
	if err != nil {
		panic(fmt.Errorf("PortableSerializer.WriteObject: %w", err))
	}
	output.WriteInt32(classDefinition.Version)
	writer := NewDefaultPortableWriter(ps, output.(*PositionalObjectDataOutput), classDefinition)
	i.(serialization.Portable).WritePortable(writer)
	writer.End()
}
