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
	"strings"

	"github.com/hazelcast/hazelcast-go-client"
)

type employee struct {
	Name       string
	Department string
}

var (
	alice = employee{
		Name:       "Alice",
		Department: "Engineering",
	}
	bob = employee{
		Name:       "Bob",
		Department: "Engineering",
	}
	john = employee{
		Name:       "John",
		Department: "Accounting",
	}
)

func main() {
	ctx := context.TODO()
	// Init client and create a map.
	c, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	m, err := c.GetMultiMap(ctx, "departments")
	if err != nil {
		log.Fatal(err)
	}
	// Map employees to departments
	if err = m.PutAll(ctx, "Engineering", alice, bob); err != nil {
		log.Fatal(err)
	}
	if _, err = m.Put(ctx, "Accounting", john); err != nil {
		log.Fatal(err)
	}
	// Check key for "Engineering".
	ok, err := m.ContainsKey(ctx, "Engineering")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("key: %s, exists: %t\n", "Engineering", ok)
	// Check key for non-existing department.
	if ok, err = m.ContainsKey(ctx, "Sales"); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("(non-existing) key: %s, exists: %t\n", "Sales", ok)
	// Check if Bob exists by value.
	if ok, err = m.ContainsValue(ctx, bob); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("value: %#v, exists: %t\n", bob, ok)
	// Print all employee names.
	allEmployees, err := m.GetValues(ctx)
	if err != nil {
		log.Fatal(err)
	}
	var names []string
	for _, e := range allEmployees {
		names = append(names, e.(employee).Name)
	}
	// Observe that order does not depend on insertion order.
	fmt.Printf("Employees: %s \n", strings.Join(names, ", "))
	// Print all keys (departments)
	keys, err := m.GetKeySet(ctx)
	if err != nil {
		log.Fatal(err)
	}
	var deps []string
	for _, d := range keys {
		deps = append(deps, d.(string))
	}
	fmt.Printf("Departments: %s \n", strings.Join(deps, ", "))
	// Delete "Engineering" department including both employees in it.
	// Observe the decrease in department count.
	if _, err = m.Remove(ctx, "Engineering"); err != nil {
		log.Fatal(err)
	}
	departmentCount, err := m.Size(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Department count: ", departmentCount)
	// Clear all departments and observe there are no entries left.
	if err = m.Clear(ctx); err != nil {
		log.Fatal(err)
	}
	entries, err := m.GetEntrySet(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range entries {
		fmt.Printf("Department:%s, Employees: %s\n", e.Key, e.Value.(employee).Name)
	}
}
