package serialization

import (
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/serialization/api"
)

type ClassDefinitionWriter struct {
	portableContext    *PortableContext
	buildingDefinition *ClassDefinition
	index              int32
	fieldDefinitions   map[string]*FieldDefinition
}

func NewClassDefinitionWriter(portableContext *PortableContext, factoryId int32, classId int32, version int32) *ClassDefinitionWriter {
	return &ClassDefinitionWriter{portableContext, NewClassDefinition(factoryId, classId, version), 0, make(map[string]*FieldDefinition)}
}

func (cdw *ClassDefinitionWriter) addFieldByType(fieldName string, fieldType int32, factoryId int32, classId int32) {
	cdw.fieldDefinitions[fieldName] = NewFieldDefinition(cdw.index, fieldName, fieldType, factoryId, classId)
	cdw.index += 1
}

func (cdw *ClassDefinitionWriter) WriteByte(fieldName string, value byte) {
	cdw.addFieldByType(fieldName, BYTE, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteBool(fieldName string, value bool) {
	cdw.addFieldByType(fieldName, BOOLEAN, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteUInt16(fieldName string, value uint16) {
	cdw.addFieldByType(fieldName, CHAR, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteInt16(fieldName string, value int16) {
	cdw.addFieldByType(fieldName, SHORT, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteInt32(fieldName string, value int32) {
	cdw.addFieldByType(fieldName, INT, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteInt64(fieldName string, value int64) {
	cdw.addFieldByType(fieldName, LONG, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteFloat32(fieldName string, value float32) {
	cdw.addFieldByType(fieldName, FLOAT, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteFloat64(fieldName string, value float64) {
	cdw.addFieldByType(fieldName, DOUBLE, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteUTF(fieldName string, value string) {
	cdw.addFieldByType(fieldName, UTF, 0, 0)
}

func (cdw *ClassDefinitionWriter) WritePortable(fieldName string, portable Portable) error {
	if portable == nil {
		return common.NewHazelcastSerializationError("cannot write nil portable without explicitly registering class definition", nil)
	}
	nestedCD, err := cdw.portableContext.LookUpOrRegisterClassDefiniton(portable)
	if err != nil {
		return err
	}
	cdw.addFieldByType(fieldName, PORTABLE, nestedCD.factoryId, nestedCD.classId)
	return nil
}

func (cdw *ClassDefinitionWriter) WriteNilPortable(fieldName string, factoryId int32, classId int32) error {
	var version int32 = 0
	nestedCD := cdw.portableContext.LookUpClassDefinition(factoryId, classId, version)
	if nestedCD == nil {
		return common.NewHazelcastSerializationError("cannot write nil portable without explicitly registering class definition", nil)
	}
	cdw.addFieldByType(fieldName, PORTABLE, nestedCD.factoryId, nestedCD.classId)
	return nil
}

func (cdw *ClassDefinitionWriter) WriteByteArray(fieldName string, value []byte) {
	cdw.addFieldByType(fieldName, BYTE_ARRAY, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteBoolArray(fieldName string, value []bool) {
	cdw.addFieldByType(fieldName, BOOLEAN_ARRAY, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteUInt16Array(fieldName string, value []uint16) {
	cdw.addFieldByType(fieldName, CHAR_ARRAY, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteInt16Array(fieldName string, value []int16) {
	cdw.addFieldByType(fieldName, SHORT_ARRAY, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteInt32Array(fieldName string, value []int32) {
	cdw.addFieldByType(fieldName, INT_ARRAY, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteInt64Array(fieldName string, value []int64) {
	cdw.addFieldByType(fieldName, LONG_ARRAY, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteFloat32Array(fieldName string, value []float32) {
	cdw.addFieldByType(fieldName, FLOAT_ARRAY, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteFloat64Array(fieldName string, value []float64) {
	cdw.addFieldByType(fieldName, DOUBLE_ARRAY, 0, 0)
}

func (cdw *ClassDefinitionWriter) WriteUTFArray(fieldName string, value []string) {
	cdw.addFieldByType(fieldName, UTF_ARRAY, 0, 0)
}

func (cdw *ClassDefinitionWriter) WritePortableArray(fieldName string, portables []Portable) error {
	if portables == nil {
		return common.NewHazelcastSerializationError("non nil value expected", nil)
	}
	if len(portables) == 0 || portables == nil {
		return common.NewHazelcastSerializationError("cannot write empty array", nil)
	}
	var sample = portables[0]
	var nestedCD, err = cdw.portableContext.LookUpOrRegisterClassDefiniton(sample)
	if err != nil {
		return nil
	}
	cdw.addFieldByType(fieldName, PORTABLE_ARRAY, nestedCD.factoryId, nestedCD.classId)
	return nil
}

func (cdw *ClassDefinitionWriter) End() {
	for _, field := range cdw.fieldDefinitions {
		cdw.buildingDefinition.addFieldDefinition(cdw.fieldDefinitions[field.fieldName])
	}
}

func (cdw *ClassDefinitionWriter) RegisterAndGet() (*ClassDefinition, error) {
	return cdw.portableContext.RegisterClassDefinition(cdw.buildingDefinition)
}

func (cdw *ClassDefinitionWriter) GetDefinition() *ClassDefinition {
	return cdw.buildingDefinition
}
