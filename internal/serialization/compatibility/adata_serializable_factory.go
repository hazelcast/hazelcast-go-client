package compatibility

import "github.com/hazelcast/go-client/internal/serialization/api"

type aDataSerializableFactory struct{}

func (*aDataSerializableFactory) Create(classId int32) api.IdentifiedDataSerializable {
	if classId == DATA_SERIALIZABLE_CLASS_ID {
		return &anIdentifiedDataSerializable{}
	}
	return nil
}
