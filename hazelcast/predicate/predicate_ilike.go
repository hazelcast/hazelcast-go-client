package predicate

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

func ILike(attributeName string, expression string) *predILike {
	return &predILike{
		attribute:  attributeName,
		expression: expression,
	}
}

type predILike struct {
	attribute  string
	expression string
}

func (p predILike) FactoryID() int32 {
	return factoryID
}

func (p predILike) ClassID() int32 {
	return 6
}

func (p *predILike) ReadData(input serialization.DataInput) error {
	p.attribute = input.ReadString()
	p.expression = input.ReadString()
	return input.Error()
}

func (p predILike) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.attribute)
	output.WriteString(p.expression)
	return nil
}

func (p predILike) String() string {
	return fmt.Sprintf("ILike(%s, %s)", p.attribute, p.expression)
}

func (p predILike) enforcePredicate() {

}
