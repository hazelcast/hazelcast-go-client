package serialization

import pubserialization "github.com/hazelcast/hazelcast-go-client/serialization"

func NewFieldDefinition(index int32, fieldName string, fieldType int32, factoryID int32,
	classID int32, version int32) pubserialization.FieldDefinition {
	return pubserialization.FieldDefinition{
		Index:     index,
		Name:      fieldName,
		Type:      fieldType,
		FactoryID: factoryID,
		ClassID:   classID,
		Version:   version,
	}
}
