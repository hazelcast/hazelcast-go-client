package serialization

import (
	"fmt"
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/serialization"
)

type MorphingPortableReader struct {
	*DefaultPortableReader
}

func NewMorphingPortableReader(portableSerializer *PortableSerializer, input DataInput, classDefinition *ClassDefinition) *MorphingPortableReader {
	return &MorphingPortableReader{NewDefaultPortableReader(portableSerializer, input, classDefinition)}
}

func (mpr *MorphingPortableReader) ReadByte(fieldName string) (byte, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	mpr.validateTypeCompatibility(fieldDef, BYTE)
	return mpr.DefaultPortableReader.ReadByte(fieldName)
}

func (mpr *MorphingPortableReader) ReadBool(fieldName string) (bool, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return false, nil
	}
	mpr.validateTypeCompatibility(fieldDef, BOOLEAN)
	return mpr.DefaultPortableReader.ReadBool(fieldName)
}

func (mpr *MorphingPortableReader) ReadUInt16(fieldName string) (uint16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	mpr.validateTypeCompatibility(fieldDef, CHAR)
	return mpr.DefaultPortableReader.ReadUInt16(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt16(fieldName string) (int16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.fieldType {
	case SHORT:
		return mpr.DefaultPortableReader.ReadInt16(fieldName)
	case BYTE:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int16(ret), err
	default:
		err := mpr.createIncompatibleClassChangeError(fieldDef, SHORT)
		return 0, err
	}
}

func (mpr *MorphingPortableReader) ReadInt32(fieldName string) (int32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.fieldType {
	case INT:
		return mpr.DefaultPortableReader.ReadInt32(fieldName)
	case BYTE:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int32(ret), err
	case CHAR:
		ret, err := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return int32(ret), err
	case SHORT:
		ret, err := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return int32(ret), err
	default:
		err := mpr.createIncompatibleClassChangeError(fieldDef, INT)
		return 0, err
	}
}

func (mpr *MorphingPortableReader) ReadInt64(fieldName string) (int64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.fieldType {
	case LONG:
		return mpr.DefaultPortableReader.ReadInt64(fieldName)
	case INT:
		ret, err := mpr.DefaultPortableReader.ReadInt32(fieldName)
		return int64(ret), err
	case BYTE:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return int64(ret), err
	case CHAR:
		ret, err := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return int64(ret), err
	case SHORT:
		ret, err := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return int64(ret), err
	default:
		err := mpr.createIncompatibleClassChangeError(fieldDef, LONG)
		return 0, err
	}
}

func (mpr *MorphingPortableReader) ReadFloat32(fieldName string) (float32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.fieldType {
	case FLOAT:
		return mpr.DefaultPortableReader.ReadFloat32(fieldName)
	case INT:
		ret, err := mpr.DefaultPortableReader.ReadInt32(fieldName)
		return float32(ret), err
	case BYTE:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return float32(ret), err
	case CHAR:
		ret, err := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return float32(ret), err
	case SHORT:
		ret, err := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return float32(ret), err
	default:
		err := mpr.createIncompatibleClassChangeError(fieldDef, FLOAT)
		return 0, err
	}
}

func (mpr *MorphingPortableReader) ReadFloat64(fieldName string) (float64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.fieldType {
	case DOUBLE:
		return mpr.DefaultPortableReader.ReadFloat64(fieldName)
	case LONG:
		ret, err := mpr.DefaultPortableReader.ReadInt64(fieldName)
		return float64(ret), err
	case FLOAT:
		ret, err := mpr.DefaultPortableReader.ReadFloat32(fieldName)
		return float64(ret), err
	case INT:
		ret, err := mpr.DefaultPortableReader.ReadInt32(fieldName)
		return float64(ret), err
	case BYTE:
		ret, err := mpr.DefaultPortableReader.ReadByte(fieldName)
		return float64(ret), err
	case CHAR:
		ret, err := mpr.DefaultPortableReader.ReadUInt16(fieldName)
		return float64(ret), err
	case SHORT:
		ret, err := mpr.DefaultPortableReader.ReadInt16(fieldName)
		return float64(ret), err
	default:
		err := mpr.createIncompatibleClassChangeError(fieldDef, DOUBLE)
		return 0, err
	}
}

func (mpr *MorphingPortableReader) ReadUTF(fieldName string) (string, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return "", nil
	}
	mpr.validateTypeCompatibility(fieldDef, UTF)
	return mpr.DefaultPortableReader.ReadUTF(fieldName)
}

func (mpr *MorphingPortableReader) ReadPortable(fieldName string) (Portable, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	mpr.validateTypeCompatibility(fieldDef, PORTABLE)
	return mpr.DefaultPortableReader.ReadPortable(fieldName)
}

func (mpr *MorphingPortableReader) ReadByteArray(fieldName string) ([]byte, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	mpr.validateTypeCompatibility(fieldDef, BYTE_ARRAY)
	return mpr.DefaultPortableReader.ReadByteArray(fieldName)
}

func (mpr *MorphingPortableReader) ReadBoolArray(fieldName string) ([]bool, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	mpr.validateTypeCompatibility(fieldDef, BOOLEAN_ARRAY)
	return mpr.DefaultPortableReader.ReadBoolArray(fieldName)
}

func (mpr *MorphingPortableReader) ReadUInt16Array(fieldName string) ([]uint16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	mpr.validateTypeCompatibility(fieldDef, CHAR_ARRAY)
	return mpr.DefaultPortableReader.ReadUInt16Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt16Array(fieldName string) ([]int16, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	mpr.validateTypeCompatibility(fieldDef, SHORT_ARRAY)
	return mpr.DefaultPortableReader.ReadInt16Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt32Array(fieldName string) ([]int32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	mpr.validateTypeCompatibility(fieldDef, INT_ARRAY)
	return mpr.DefaultPortableReader.ReadInt32Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadInt64Array(fieldName string) ([]int64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	mpr.validateTypeCompatibility(fieldDef, LONG_ARRAY)
	return mpr.DefaultPortableReader.ReadInt64Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadFloat32Array(fieldName string) ([]float32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	mpr.validateTypeCompatibility(fieldDef, FLOAT_ARRAY)
	return mpr.DefaultPortableReader.ReadFloat32Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadFloat64Array(fieldName string) ([]float64, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	mpr.validateTypeCompatibility(fieldDef, DOUBLE_ARRAY)
	return mpr.DefaultPortableReader.ReadFloat64Array(fieldName)
}

func (mpr *MorphingPortableReader) ReadUTfArray(fieldName string) ([]string, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	mpr.validateTypeCompatibility(fieldDef, UTF_ARRAY)
	return mpr.DefaultPortableReader.ReadUTFArray(fieldName)
}

func (mpr *MorphingPortableReader) ReadPortableArray(fieldName string) ([]Portable, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return nil, nil
	}
	mpr.validateTypeCompatibility(fieldDef, PORTABLE_ARRAY)
	return mpr.DefaultPortableReader.ReadPortableArray(fieldName)
}

func (mpr *MorphingPortableReader) createIncompatibleClassChangeError(fd *FieldDefinition, expectedType int32) error {
	return common.NewHazelcastSerializationError(fmt.Sprintf("incompatible to read %v from %v while reading field : %v", getTypeByConst(expectedType), getTypeByConst(fd.fieldType), fd.fieldName), nil)
}

func (mpr *MorphingPortableReader) validateTypeCompatibility(fd *FieldDefinition, expectedType int32) error {
	if fd.fieldType != expectedType {
		return mpr.createIncompatibleClassChangeError(fd, expectedType)
	}
	return nil
}
