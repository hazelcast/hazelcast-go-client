/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
	config := hazelcast.NewConfig()
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
	// Store an object in the map
	emp := Employee{"Doe"}
	m.Put(ctx, "employee-1", emp)
	// Retrieve the object and print
	value, err := m.Get(ctx, "employee-1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Surname of stored employee:", value.(Employee).Surname)
	// Shutdown client
	client.Shutdown(ctx)
}
