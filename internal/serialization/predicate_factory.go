package serialization

import . "github.com/hazelcast/go-client/internal/serialization/api"

type PredicateFactory struct {
	IdToDataSerializable map[int32]IdentifiedDataSerializable
}

func NewPredicateFactory(ids map[int32]IdentifiedDataSerializable) PredicateFactory {
	return PredicateFactory{ids}
}

func (pf *PredicateFactory) Create(id int32) IdentifiedDataSerializable {
	if pf.IdToDataSerializable[id] != nil {
		return pf.IdToDataSerializable[id]
	}
	return nil
}
