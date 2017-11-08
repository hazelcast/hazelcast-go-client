package compatibility

import . "github.com/hazelcast/go-client/serialization"

type aDataSerializableFactory struct{}

func (*aDataSerializableFactory) Create(classId int32) IdentifiedDataSerializable {
	if classId == DATA_SERIALIZABLE_CLASS_ID {
		return &anIdentifiedDataSerializable{}
	}
	return nil
}
