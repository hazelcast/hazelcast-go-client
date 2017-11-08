package serialization

import (
	"errors"
	. "github.com/hazelcast/go-client/serialization"
)

type MorphingPortableReader struct {
	*DefaultPortableReader
}

func NewMorphingPortableReader(portableSerializer *PortableSerializer, input DataInput, classDefinition *ClassDefinition) *MorphingPortableReader {
	return &MorphingPortableReader{NewDefaultPortableReader(portableSerializer, input, classDefinition)}
}

func (mpr *MorphingPortableReader) ReadInt32(fieldName string) (int32, error) {
	fieldDef := mpr.DefaultPortableReader.classDefinition.fields[fieldName]
	if fieldDef == nil {
		return 0, nil
	}
	switch fieldDef.fieldType {
	case INT:
		return mpr.DefaultPortableReader.ReadInt32(fieldName)
		//case FieldType.BYTE: return super.readByte(fieldName);
		//case FieldType.CHAR: return super.readChar(fieldName).charCodeAt(0);
		//case FieldType.SHORT: return super.readShort(fieldName);
	default:
		err := mpr.createIncompatibleClassChangeError(fieldDef, INT)
		return 0, err

	}
}

func (mpr *MorphingPortableReader) createIncompatibleClassChangeError(fd *FieldDefinition, expectedType int32) error {
	return errors.New("Incompatible to read ${expectedType} from ${fd.getType()} while reading field : ${fd.getName()}")
}
