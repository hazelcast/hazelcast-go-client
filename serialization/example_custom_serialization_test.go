/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package serialization_test

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type Employee struct {
	Surname string
}

type EmployeeCustomSerializer struct{}

func (e EmployeeCustomSerializer) ID() int32 {
	return 45392
}

func (e EmployeeCustomSerializer) Read(input serialization.DataInput) interface{} {
	surname := input.ReadString()
	return Employee{Surname: surname}
}

func (e EmployeeCustomSerializer) Write(output serialization.DataOutput, object interface{}) {
	v := object.(Employee)
	output.WriteString(v.Surname)
}

func ExampleConfig_SetCustomSerializer() {
	// Configure serializer
	config := hazelcast.NewConfig()
	if err := config.Serialization.SetCustomSerializer(reflect.TypeOf(&Employee{}), &EmployeeCustomSerializer{}); err != nil {
		panic(err)
	}
	ctx := context.TODO()
	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	m, err := client.GetMap(ctx, "map-1")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := m.Put(ctx, "employee-1", Employee{"Doe"}); err != nil {
		panic(err)
	}
	value, err := m.Get(ctx, "employee-1")
	if err != nil {
		panic(err)
	}
	fmt.Println("Surname of stored employee:", value.(Employee).Surname)
}
