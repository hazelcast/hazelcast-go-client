package pred

import (
	"fmt"
	"strings"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

func Or(predicates ...Predicate) *predOr {
	return &predOr{predicates: predicates}
}

type predOr struct {
	predicates []Predicate
}

func (p predOr) FactoryID() int32 {
	return factoryID
}

func (p predOr) ClassID() int32 {
	return 11
}

func (p *predOr) ReadData(input serialization.DataInput) error {
	length := input.ReadInt32()
	predicates := make([]Predicate, length)
	for i := 0; i < int(length); i++ {
		pred := input.ReadObject()
		predicates[i] = pred.(Predicate)
	}
	p.predicates = predicates
	return input.Error()
}

func (p predOr) WriteData(output serialization.DataOutput) error {
	output.WriteInt32(int32(len(p.predicates)))
	for _, pred := range p.predicates {
		err := output.WriteObject(pred)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p predOr) String() string {
	ps := make([]string, len(p.predicates))
	for i, pr := range p.predicates {
		ps[i] = pr.String()
	}
	return fmt.Sprintf("Or(%s)", strings.Join(ps, ", "))
}

func (p predOr) enforcePredicate() {

}
