package predicate

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

func Like(attributeName string, expression string) *predLike {
	return &predLike{
		attribute:  attributeName,
		expression: expression,
	}
}

type predLike struct {
	attribute  string
	expression string
}

func (p predLike) FactoryID() int32 {
	return factoryID
}

func (p predLike) ClassID() int32 {
	return 5
}

func (p *predLike) ReadData(input serialization.DataInput) error {
	p.attribute = input.ReadString()
	p.expression = input.ReadString()
	return input.Error()
}

func (p predLike) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.attribute)
	output.WriteString(p.expression)
	return nil
}

func (p predLike) String() string {
	return fmt.Sprintf("Like(%s, %s)", p.attribute, p.expression)
}

func (p predLike) enforcePredicate() {

}
