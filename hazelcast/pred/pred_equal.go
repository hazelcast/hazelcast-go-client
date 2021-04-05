package pred

import (
	"fmt"

	serialization "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

func Equal(fieldName string, value interface{}) *predEqual {
	return &predEqual{
		field: fieldName,
		value: value,
	}
}

type predEqual struct {
	field string
	value interface{}
}

func (p predEqual) FactoryID() int32 {
	return factoryID
}

func (p predEqual) ClassID() int32 {
	return equalID
}

func (p *predEqual) ReadData(input serialization.DataInput) error {
	p.field = input.ReadString()
	p.value = input.ReadObject()
	return input.Error()
}

func (p predEqual) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.field)
	return output.WriteObject(p.value)
}

func (p predEqual) String() string {
	return fmt.Sprintf("%s=%v", p.field, p.value)
}

func (p predEqual) enforcePredicate() {

}
