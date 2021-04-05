package pred

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

func Regex(attributeName string, expression string) *predRegex {
	return &predRegex{
		attribute:  attributeName,
		expression: expression,
	}
}

type predRegex struct {
	attribute  string
	expression string
}

func (p predRegex) FactoryID() int32 {
	return factoryID
}

func (p predRegex) ClassID() int32 {
	return 12
}

func (p *predRegex) ReadData(input serialization.DataInput) error {
	p.attribute = input.ReadString()
	p.expression = input.ReadString()
	return input.Error()
}

func (p predRegex) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.attribute)
	output.WriteString(p.expression)
	return nil
}

func (p predRegex) String() string {
	return fmt.Sprintf("Regex(%s, %s)", p.attribute, p.expression)
}

func (p predRegex) enforcePredicate() {

}
