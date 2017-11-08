package serialization

import . "github.com/hazelcast/go-client/serialization"

type PredicateFactory struct {
	idToDataSerializable map[int32]IdentifiedDataSerializable
}

func NewPredicateFactory(ids map[int32]IdentifiedDataSerializable) PredicateFactory {
	return PredicateFactory{ids}
}

func (pf *PredicateFactory) Create(id int32) IdentifiedDataSerializable {
	if pf.idToDataSerializable[id] != nil {
		return pf.idToDataSerializable[id]
	}
	return nil
}
