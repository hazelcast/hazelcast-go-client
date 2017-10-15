package compatibility

import "github.com/hazelcast/go-client/internal/serialization/api"

type aPortableFactory struct{}

func (pf *aPortableFactory) Create(classId int32) api.Portable {
	if classId == PORTABLE_CLASS_ID {
		return &aPortable{}
	} else if classId == INNER_PORTABLE_CLASS_ID {
		return &AnInnerPortable{}
	}
	return nil
}
