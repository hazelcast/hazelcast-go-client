package predicate

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

func GreaterOrEqual(attributeName string, value interface{}) *predGreaterOrEqual {
	return &predGreaterOrEqual{
		attribute: attributeName,
		value:     value,
	}
}

type predGreaterOrEqual struct {
	attribute string
	value     interface{}
}

func (p predGreaterOrEqual) FactoryID() int32 {
	return factoryID
}

func (p predGreaterOrEqual) ClassID() int32 {
	return 4
}

func (p *predGreaterOrEqual) ReadData(input serialization.DataInput) error {
	p.attribute = input.ReadString()
	p.value = input.ReadObject()
	return input.Error()
}

func (p predGreaterOrEqual) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.attribute)
	return output.WriteObject(p.value)
}

func (p predGreaterOrEqual) String() string {
	return fmt.Sprintf("%s>=%v", p.attribute, p.value)
}

func (p predGreaterOrEqual) enforcePredicate() {

}
