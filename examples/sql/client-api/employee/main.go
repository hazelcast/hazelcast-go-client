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

/*
This example demonstrates how to use the SQL methods of the Hazelcast Go Client.
*/

package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/sql"
)

var names = []string{"Gorkem", "Ezgi", "Joe", "Jane", "Mike", "Mandy", "Tom", "Tina"}
var surnames = []string{"Tekol", "Brown", "Taylor", "McGregor", "Bronson"}

type Employee struct {
	Name string
	Age  int16
}

// createMapping creates the mapping for the given map name.
func createMapping(client *hazelcast.Client, mapName string) error {
	q := fmt.Sprintf(`
        CREATE MAPPING IF NOT EXISTS "%s" (
			__key BIGINT,
			age BIGINT,
			name VARCHAR
		)
        TYPE IMAP 
        OPTIONS (
            'keyFormat' = 'bigint',
            'valueFormat' = 'json-flat'
        )
`, mapName)
	result, err := client.SQL().Execute(context.Background(), q)
	if err != nil {
		return fmt.Errorf("creating mapping: %w", err)
	}
	return result.Close()
}

// populateMap creates entries in the given map.
// It uses SINK INTO instead of INSERT INTO in order to update already existing entries.
func populateMap(client *hazelcast.Client, mapName string, employees []Employee) error {
	q := fmt.Sprintf(`SINK INTO "%s"(__key, age, name) VALUES (?, ?, ?)`, mapName)
	for i, e := range employees {
		if _, err := client.SQL().Execute(context.Background(), q, i, e.Age, e.Name); err != nil {
			return fmt.Errorf("populating map: %w", err)
		}
	}
	return nil
}

// queryMap returns employees with the given minimum age.
func queryMap(client *hazelcast.Client, mapName string, minAge int) ([]Employee, error) {
	stmt := sql.NewStatement(fmt.Sprintf(`SELECT name, CAST(age AS smallint) FROM "%s" WHERE age >= ?`, mapName))
	stmt.AddParameter(minAge)
	result, err := client.SQL().ExecuteStatement(context.Background(), stmt)
	if err != nil {
		return nil, fmt.Errorf("querying: %w", err)
	}
	defer result.Close()
	iter, err := result.Iterator()
	if err != nil {
		return nil, fmt.Errorf("acquaring iterator: %w", err)
	}
	var emps []Employee
	for iter.HasNext() {
		e := Employee{}
		row, err := iter.Next()
		if err != nil {
			return nil, fmt.Errorf("iterating rows: %w", err)
		}
		name, err := row.Get(0)
		if err != nil {
			return nil, fmt.Errorf("accessing row field: %w", err)
		}
		e.Name = name.(string)
		age, err := row.Get(1)
		if err != nil {
			return nil, fmt.Errorf("accessing row field: %w", err)
		}
		// we can check for column type
		c, err := row.Metadata().GetColumn(1)
		if err != nil {
			return nil, fmt.Errorf("accessing metadata: %w", err)
		}
		if c.Type() != sql.ColumnTypeSmallInt {
			return nil, fmt.Errorf("unexpected column type: %d", c.Type())
		}
		// we already check that it is int16
		e.Age = age.(int16)
		emps = append(emps, e)
	}
	return emps, nil
}

// randomAge returns a random age.
func randomAge() int16 {
	return int16(rand.Intn(40) + 20)
}

// randomName returns a random name + surname.
func randomName() string {
	name := names[rand.Intn(len(names))]
	surname := surnames[rand.Intn(len(surnames))]
	return fmt.Sprintf("%s %s", name, surname)
}

// randomEmployees creates count random employees.
func randomEmployees(count int) []Employee {
	emps := make([]Employee, 0, count)
	for i := 0; i < count; i++ {
		e := Employee{
			Age:  randomAge(),
			Name: randomName(),
		}
		emps = append(emps, e)
	}
	return emps
}

func main() {
	// Connect to the local Hazelcast server.
	// Uses the unisocket option just for demonstration.
	client, err := hazelcast.StartNewClient(context.Background())
	if err != nil {
		panic(fmt.Errorf("creating the client: %w", err))
	}
	// Don't forget to close the database.
	defer client.Shutdown(context.Background())
	const mapName = "employees"
	// Seed the random number generator.
	rand.Seed(time.Now().UnixNano())
	// Creating the mapping is required only once.
	if err := createMapping(client, mapName); err != nil {
		panic(err)
	}
	if err := populateMap(client, mapName, randomEmployees(10)); err != nil {
		panic(err)
	}
	emps, err := queryMap(client, mapName, 40)
	if err != nil {
		panic(err)
	}
	fmt.Println(emps)
}
