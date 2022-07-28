package serialization_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	factoryID         = 1
	employeeClassID   = 1
	baseObjectClassID = 2
)

type EmployeePortable struct {
	Name string
	Age  int32
}

func (e EmployeePortable) FactoryID() int32 {
	return factoryID
}

func (e EmployeePortable) ClassID() int32 {
	return employeeClassID
}

func (e EmployeePortable) WritePortable(writer serialization.PortableWriter) {
	writer.WriteString("name", e.Name)
	writer.WriteInt32("age", e.Age)
}

func (e *EmployeePortable) ReadPortable(reader serialization.PortableReader) {
	e.Name = reader.ReadString("name")
	e.Age = reader.ReadInt32("age")
}

type MyPortableFactory struct {
}

func (m MyPortableFactory) Create(classID int32) serialization.Portable {
	if classID == employeeClassID {
		return &EmployeePortable{}
	}
	if classID == baseObjectClassID {
		return &BaseObject{}
	}
	return nil
}

func (m MyPortableFactory) FactoryID() int32 {
	return factoryID
}

type BaseObject struct {
	Employee *EmployeePortable
}

func (b BaseObject) FactoryID() int32 {
	return factoryID
}

func (b BaseObject) ClassID() int32 {
	return 2
}

func (b BaseObject) WritePortable(writer serialization.PortableWriter) {
	if b.Employee == nil {
		writer.WriteNilPortable("employee", factoryID, employeeClassID)
		return
	}
	writer.WritePortable("employee", b.Employee)
}

func (b *BaseObject) ReadPortable(reader serialization.PortableReader) {
	b.Employee = reader.ReadPortable("employee").(*EmployeePortable)
}

func ExampleConfig_SetPortableFactories() {
	// create the configuration
	config := hazelcast.NewConfig()
	config.Serialization.PortableVersion = 1
	config.Serialization.SetPortableFactories(&MyPortableFactory{})
	// version must be equivalent to PortableVersion
	employeeCD := serialization.NewClassDefinition(factoryID, employeeClassID, 1)
	employeeCD.AddStringField("name")
	employeeCD.AddInt32Field("age")
	baseObjectCD := serialization.NewClassDefinition(factoryID, baseObjectClassID, 1)
	baseObjectCD.AddPortableField("employee", employeeCD)
	config.Serialization.SetClassDefinitions(employeeCD, baseObjectCD)

	// start the client with the given configuration
	ctx := context.TODO()
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	// retrieve a map
	m, err := client.GetMap(ctx, "example")
	if err != nil {
		log.Fatal(err)
	}
	// set / get portable serialized value
	employee := &EmployeePortable{Name: "Jane Doe", Age: 30}
	if err := m.Set(ctx, "employee", employee); err != nil {
		log.Fatal(err)
	}
	if v, err := m.Get(ctx, "employee"); err != nil {
		log.Fatal(err)
	} else {
		fmt.Println(v)
	}
	// set / get portable serialized nullable value
	baseObj := &BaseObject{Employee: employee}
	if err := m.Set(ctx, "base-obj", baseObj); err != nil {
		log.Fatal(err)
	}
	if v, err := m.Get(ctx, "base-obj"); err != nil {
		log.Fatal(err)
	} else {
		fmt.Println(v)
	}
	// stop the client
	time.Sleep(1 * time.Second)
	client.Shutdown(ctx)
}
