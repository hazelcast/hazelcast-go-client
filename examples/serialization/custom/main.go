package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type Employee struct {
	Surname string
}

type EmployeeCustomSerializer struct{}

func (e EmployeeCustomSerializer) ID() (id int32) {
	return 45392
}

func (e EmployeeCustomSerializer) Read(input serialization.DataInput) interface{} {
	surname := input.ReadString()
	return &Employee{Surname: surname}
}

func (e EmployeeCustomSerializer) Write(output serialization.DataOutput, object interface{}) {
	employee, ok := object.(*Employee)
	if !ok {
		panic("can serialize only Employee")
	}
	output.WriteString(employee.Surname)
}

func main() {
	config := hazelcast.NewConfig()
	config.Serialization.SetCustomSerializer(reflect.TypeOf(&Employee{}), &EmployeeCustomSerializer{})

	// Start the client
	ctx := context.TODO()
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}

	// Get a random map
	rand.Seed(time.Now().Unix())
	mapName := fmt.Sprintf("sample-%d", rand.Int())
	m, err := client.GetMap(ctx, mapName)
	if err != nil {
		log.Fatal(err)
	}

	emp := Employee{"Doe"}
	m.Put(ctx, "employee-1", emp)

	value, err := m.Get(ctx, "employee-1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Surname of stored employee:", value.(Employee).Surname)
}
