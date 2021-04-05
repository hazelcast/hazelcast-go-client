package pred

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

func Not(predicate Predicate) *predNot {
	return &predNot{
		pred: predicate,
	}
}

type predNot struct {
	pred Predicate
}

func (p predNot) FactoryID() int32 {
	return factoryID
}

func (p predNot) ClassID() int32 {
	return 10
}

func (p *predNot) ReadData(input serialization.DataInput) error {
	p.pred = input.ReadObject().(Predicate)
	return input.Error()
}

func (p predNot) WriteData(output serialization.DataOutput) error {
	return output.WriteObject(p.pred)
}

func (p predNot) String() string {
	return fmt.Sprintf("~%v", p.pred)
}

func (p predNot) enforcePredicate() {

}
