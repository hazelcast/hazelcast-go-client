package predicate

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

func Between(attribute string, from interface{}, to interface{}) *predBetween {
	return &predBetween{
		attribute: attribute,
		from:      from,
		to:        to,
	}
}

type predBetween struct {
	attribute string
	from      interface{}
	to        interface{}
}

func (p predBetween) FactoryID() int32 {
	return factoryID
}

func (p predBetween) ClassID() int32 {
	return 2
}

func (p *predBetween) ReadData(input serialization.DataInput) error {
	p.attribute = input.ReadString()
	p.from = input.ReadObject()
	p.to = input.ReadObject()
	return input.Error()
}

func (p predBetween) WriteData(output serialization.DataOutput) error {
	var err error
	output.WriteString(p.attribute)
	err = output.WriteObject(p.from)
	if err != nil {
		return err
	}
	return output.WriteObject(p.to)
}

func (p predBetween) String() string {
	return fmt.Sprintf("Between('%s', %#v, %#v)", p.attribute, p.from, p.to)
}

func (p predBetween) enforcePredicate() {

}
