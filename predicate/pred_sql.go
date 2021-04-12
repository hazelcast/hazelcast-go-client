package predicate

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func SQL(expression string) *predSQL {
	return &predSQL{
		expression: expression,
	}
}

type predSQL struct {
	expression string
}

func (p predSQL) FactoryID() int32 {
	return factoryID
}

func (p predSQL) ClassID() int32 {
	return 0
}

func (p *predSQL) ReadData(input serialization.DataInput) error {
	p.expression = input.ReadString()
	return input.Error()
}

func (p predSQL) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.expression)
	return nil
}

func (p predSQL) String() string {
	return fmt.Sprintf("SQL(%s)", p.expression)
}

func (p predSQL) enforcePredicate() {

}
