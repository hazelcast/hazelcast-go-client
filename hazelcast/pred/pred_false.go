package pred

import "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"

func False(predicate Predicate) *predFalse {
	return &predFalse{}
}

type predFalse struct {
}

func (p predFalse) FactoryID() int32 {
	return factoryID
}

func (p predFalse) ClassID() int32 {
	return 13
}

func (p *predFalse) ReadData(input serialization.DataInput) error {
	return nil
}

func (p predFalse) WriteData(output serialization.DataOutput) error {
	return nil
}

func (p predFalse) String() string {
	return "(false)"
}

func (p predFalse) enforcePredicate() {

}
