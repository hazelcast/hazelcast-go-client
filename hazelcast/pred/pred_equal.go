package pred

import (
	"fmt"

	serialization "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

func Equal(attributeName string, value interface{}) *predEqual {
	return &predEqual{
		attribute: attributeName,
		value:     value,
	}
}

type predEqual struct {
	attribute string
	value     interface{}
}

func (p predEqual) FactoryID() int32 {
	return factoryID
}

func (p predEqual) ClassID() int32 {
	return 3
}

func (p *predEqual) ReadData(input serialization.DataInput) error {
	p.attribute = input.ReadString()
	p.value = input.ReadObject()
	return input.Error()
}

func (p predEqual) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.attribute)
	return output.WriteObject(p.value)
}

func (p predEqual) String() string {
	return fmt.Sprintf("%s=%v", p.attribute, p.value)
}

func (p predEqual) enforcePredicate() {

}
