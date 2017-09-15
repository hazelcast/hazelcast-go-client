package serialization

type ClassDefinition struct {
	factoryId int32
	classId   int32
	version   int32
	fields    map[string]*FieldDefinition
}

func NewClassDefinition(factoryId int32, classId int32, version int32) *ClassDefinition {
	return &ClassDefinition{factoryId, classId, version, make(map[string]*FieldDefinition)}
}

func (cd *ClassDefinition) addFieldDefinition(definition *FieldDefinition) {
	cd.fields[definition.fieldName] = definition
}

type FieldDefinition struct {
	index     int32
	fieldName string
	fieldType int32
	factoryId int32
	classId   int32
}

func NewFieldDefinition(index int32, fieldName string, fieldType int32, factoryId int32, classId int32) *FieldDefinition {
	return &FieldDefinition{index, fieldName, fieldType, factoryId, classId}
}

const (
	PORTABLE       = 0
	BYTE           = 1
	BOOLEAN        = 2
	CHAR           = 3
	SHORT          = 4
	INT            = 5
	LONG           = 6
	FLOAT          = 7
	DOUBLE         = 8
	UTF            = 9
	PORTABLE_ARRAY = 10
	BYTE_ARRAY     = 11
	BOOLEAN_ARRAY  = 12
	CHAR_ARRAY     = 13
	SHORT_ARRAY    = 14
	INT_ARRAY      = 15
	LONG_ARRAY     = 16
	FLOAT_ARRAY    = 17
	DOUBLE_ARRAY   = 18
	UTF_ARRAY      = 19
)
