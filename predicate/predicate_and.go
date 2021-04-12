package predicate

import (
	"fmt"
	"strings"

	serialization "github.com/hazelcast/hazelcast-go-client/serialization"
)

func And(predicates ...Predicate) *predAnd {
	return &predAnd{predicates: predicates}
}

type predAnd struct {
	predicates []Predicate
}

func (p predAnd) FactoryID() int32 {
	return factoryID
}

func (p predAnd) ClassID() int32 {
	return 1
}

func (p *predAnd) ReadData(input serialization.DataInput) error {
	length := input.ReadInt32()
	predicates := make([]Predicate, length)
	for i := 0; i < int(length); i++ {
		pred := input.ReadObject()
		predicates[i] = pred.(Predicate)
	}
	p.predicates = predicates
	return input.Error()
}

func (p predAnd) WriteData(output serialization.DataOutput) error {
	output.WriteInt32(int32(len(p.predicates)))
	for _, pred := range p.predicates {
		err := output.WriteObject(pred)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p predAnd) String() string {
	ps := make([]string, len(p.predicates))
	for i, pr := range p.predicates {
		ps[i] = pr.String()
	}
	return fmt.Sprintf("And(%s)", strings.Join(ps, ", "))
}

func (p predAnd) enforcePredicate() {

}
