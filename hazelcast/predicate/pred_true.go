package predicate

import "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"

func True(predicate Predicate) *predTrue {
	return &predTrue{}
}

type predTrue struct {
}

func (p predTrue) FactoryID() int32 {
	return factoryID
}

func (p predTrue) ClassID() int32 {
	return 14
}

func (p *predTrue) ReadData(input serialization.DataInput) error {
	return nil
}

func (p predTrue) WriteData(output serialization.DataOutput) error {
	return nil
}

func (p predTrue) String() string {
	return "(false)"
}

func (p predTrue) enforcePredicate() {

}
