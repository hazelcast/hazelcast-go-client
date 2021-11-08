// +build ignore

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
	// Configure serializer
	config := hazelcast.Config{}
	config.Serialization.SetCustomSerializer(reflect.TypeOf(&Employee{}), &EmployeeCustomSerializer{})
	// Start the client with custom serializer
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
	// Store an object in the map, error handling is ommitted here for brevity
	m.Put(ctx, "employee-1", Employee{"Doe"})
	// Retrieve the object and print
	value, err := m.Get(ctx, "employee-1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Surname of stored employee:", value.(Employee).Surname)
	// Shutdown client
	client.Shutdown(ctx)
}
