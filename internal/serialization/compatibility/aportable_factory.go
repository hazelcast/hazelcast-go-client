package compatibility

import . "github.com/hazelcast/go-client/serialization"

type aPortableFactory struct{}

func (pf *aPortableFactory) Create(classId int32) Portable {
	if classId == PORTABLE_CLASS_ID {
		return &aPortable{}
	} else if classId == INNER_PORTABLE_CLASS_ID {
		return &AnInnerPortable{}
	}
	return nil
}
