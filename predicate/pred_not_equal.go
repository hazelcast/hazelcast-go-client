package predicate

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func NotEqual(attributeName string, value interface{}) *predNotEqual {
	return &predNotEqual{
		attribute: attributeName,
		value:     value,
	}
}

type predNotEqual struct {
	attribute string
	value     interface{}
}

func (p predNotEqual) FactoryID() int32 {
	return factoryID
}

func (p predNotEqual) ClassID() int32 {
	return 9
}

func (p *predNotEqual) ReadData(input serialization.DataInput) error {
	p.attribute = input.ReadString()
	p.value = input.ReadObject()
	return input.Error()
}

func (p predNotEqual) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.attribute)
	return output.WriteObject(p.value)
}

func (p predNotEqual) String() string {
	return fmt.Sprintf("%s!=%v", p.attribute, p.value)
}

func (p predNotEqual) enforcePredicate() {

}
