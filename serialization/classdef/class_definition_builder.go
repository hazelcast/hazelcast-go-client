package classdef

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/core"
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization/classdef"
	. "github.com/hazelcast/hazelcast-go-client/serialization"
)

type ClassDefinitionBuilder struct {
	factoryId        int32
	classId          int32
	version          int32
	fieldDefinitions map[string]FieldDefinition
	index            int32
	done             bool
}

func NewClassDefinitionBuilder(factoryId int32, classId int32, version int32) *ClassDefinitionBuilder {
	return &ClassDefinitionBuilder{factoryId, classId, version, make(map[string]FieldDefinition), 0, false}
}

func (cdb *ClassDefinitionBuilder) AddByteField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, BYTE, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddBoolField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, BOOL, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddUInt16Field(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, UINT16, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddInt16Field(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, INT16, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddInt32Field(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, INT32, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddInt64Field(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, INT64, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddFloat32Field(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, FLOAT32, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddFloat64Field(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, FLOAT64, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddUTFField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, UTF, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddPortableField(fieldName string, def ClassDefinition) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	if def.ClassId() == 0 {
		return core.NewHazelcastIllegalArgumentError("Portable class id cannot be zero", nil)
	}

	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, PORTABLE, def.FactoryId(), def.ClassId(), cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddByteArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, BYTE_ARRAY, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddBoolArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, BOOL_ARRAY, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddUInt16ArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, UINT16_ARRAY, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddInt16ArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, INT16_ARRAY, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddInt32ArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, INT32_ARRAY, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddInt64ArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, INT64_ARRAY, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddFloat32ArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, FLOAT32_ARRAY, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddFloat64ArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, FLOAT64_ARRAY, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddUTFArrayField(fieldName string) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, UTF_ARRAY, 0, 0, cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddPortableArrayField(fieldName string, def ClassDefinition) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	if def.ClassId() == 0 {
		return core.NewHazelcastIllegalArgumentError("Portable class id cannot be zero", nil)
	}

	cdb.fieldDefinitions[fieldName] = NewFieldDefinitionImpl(cdb.index, fieldName, PORTABLE_ARRAY, def.FactoryId(), def.ClassId(), cdb.version)
	cdb.index++
	return nil
}

func (cdb *ClassDefinitionBuilder) AddField(fieldDefinition FieldDefinition) error {
	err := cdb.check()
	if err != nil {
		return err
	}
	if cdb.index != fieldDefinition.Index() {
		return core.NewHazelcastIllegalArgumentError("invalid field index", nil)
	}
	cdb.index++
	cdb.fieldDefinitions[fieldDefinition.Name()] = fieldDefinition
	return nil
}

func (cdb *ClassDefinitionBuilder) Build() ClassDefinition {
	cdb.done = true
	cd := NewClassDefinitionImpl(cdb.factoryId, cdb.classId, cdb.version)
	for _, fd := range cdb.fieldDefinitions {
		cd.AddFieldDefinition(fd)
	}
	return cd
}

func (cdb *ClassDefinitionBuilder) check() error {
	if cdb.done {
		return core.NewHazelcastSerializationError(fmt.Sprintf("ClassDefinition is already built for %v", cdb.classId), nil)
	}
	return nil
}
