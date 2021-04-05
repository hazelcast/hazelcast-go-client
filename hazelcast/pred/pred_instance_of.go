package pred

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/serialization"
)

func InstanceOf(className string) *predInstanceOf {
	return &predInstanceOf{
		className: className,
	}
}

type predInstanceOf struct {
	className string
}

func (p predInstanceOf) FactoryID() int32 {
	return factoryID
}

func (p predInstanceOf) ClassID() int32 {
	return 8
}

func (p *predInstanceOf) ReadData(input serialization.DataInput) error {
	p.className = input.ReadString()
	return input.Error()
}

func (p predInstanceOf) WriteData(output serialization.DataOutput) error {
	output.WriteString(p.className)
	return nil
}

func (p predInstanceOf) String() string {
	return fmt.Sprintf("InstanceOf(%s)", p.className)
}

func (p predInstanceOf) enforcePredicate() {

}
