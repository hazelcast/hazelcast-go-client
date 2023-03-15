/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
	"reflect"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

// Person is a simple type that contains information about a person.
type Person struct {
	Name string
	Age  int32
}

func (p Person) String() string {
	return fmt.Sprintf("Name: %s, Age: %d", p.Name, p.Age)
}

// PersonSerializer serializes a Person value using compact serialization.
type PersonSerializer struct{}

// Type returns the target type: Person
func (s PersonSerializer) Type() reflect.Type {
	return reflect.TypeOf(Person{})
}

// TypeName returns and identifier for the serialized type.
func (s PersonSerializer) TypeName() string {
	return "Person"
}

// Read reads a Person value from compact serialized data.
func (s PersonSerializer) Read(r serialization.CompactReader) interface{} {
	var name string
	p := r.ReadString("name")
	if p != nil {
		name = *p
	}
	return Person{
		Name: name,
		Age:  r.ReadInt32("age"),
	}
}

// Write writes a Person value as compact serialized data.
func (s PersonSerializer) Write(w serialization.CompactWriter, value interface{}) {
	v := value.(Person)
	w.WriteString("name", &v.Name)
	w.WriteInt32("age", v.Age)
}

func main() {
	ctx := context.Background()
	// create the configuration and set the compact serializers.
	var cfg hazelcast.Config
	cfg.Serialization.Compact.SetSerializers(PersonSerializer{})
	// start the client with the configuration.
	client, err := hazelcast.StartNewClientWithConfig(ctx, cfg)
	if err != nil {
		panic(fmt.Errorf("starting the client: %w", err))
	}
	// get the sample map, so we can call Get and Set operations on it.
	m, err := client.GetMap(ctx, "people")
	if err != nil {
		panic(fmt.Errorf("getting the map: %w", err))
	}
	person := Person{
		Name: "Jane",
		Age:  25,
	}
	// Set the person value in the map.
	if err := m.Set(ctx, "jane", person); err != nil {
		panic(fmt.Errorf("setting the value: %w", err))
	}
	// Get the value back from the map.
	value, err := m.Get(ctx, "jane")
	if err != nil {
		panic(fmt.Errorf("getting the value: %w", err))
	}
	fmt.Println("read the value back: ", value)
}
