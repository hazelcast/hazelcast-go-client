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
This example demonstrates how to use the Hazelcast database/sql driver.
*/

package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	_ "github.com/hazelcast/hazelcast-go-client/sql/driver"
)

var names = []string{"Gorkem", "Ezgi", "Joe", "Jane", "Mike", "Mandy", "Tom", "Tina"}
var surnames = []string{"Tekol", "Brown", "Taylor", "McGregor", "Bronson"}

type Employee struct {
	Name string
	Age  int16
}

// createMapping creates the mapping for the given map name.
func createMapping(db *sql.DB, mapName string) error {
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
	_, err := db.Exec(q)
	if err != nil {
		return fmt.Errorf("error creating mapping: %w", err)
	}
	return nil
}

// populateMap creates entries in the given map.
// It uses SINK INTO instead of INSERT INTO in order to update already existing entries.
func populateMap(db *sql.DB, mapName string, employess []Employee) error {
	q := fmt.Sprintf(`SINK INTO "%s"(__key, age, name) VALUES (?, ?, ?)`, mapName)
	for i, e := range employess {
		if _, err := db.Exec(q, i, e.Age, e.Name); err != nil {
			return fmt.Errorf("populating map: %w", err)
		}
	}
	return nil
}

// queryMap returns employees with the given minimum age.
func queryMap(db *sql.DB, mapName string, minAge int) ([]Employee, error) {
	q := fmt.Sprintf(`SELECT name, age FROM "%s" WHERE age >= ?`, mapName)
	rows, err := db.Query(q, minAge)
	if err != nil {
		return nil, fmt.Errorf("error querying: %w", err)
	}
	defer rows.Close()
	var emps []Employee
	for rows.Next() {
		e := Employee{}
		if err := rows.Scan(&e.Name, &e.Age); err != nil {
			return nil, fmt.Errorf("error scanning: %w", err)
		}
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
	emps := make([]Employee, count)
	for i := 0; i < count; i++ {
		e := Employee{
			Age:  randomAge(),
			Name: randomName(),
		}
		emps[i] = e
	}
	return emps
}

func main() {
	// Connect to the local Hazelcast server.
	// Uses the unisocket option just for demonstration.
	db, err := sql.Open("hazelcast", "hz://localhost:5701?unisocket=true")
	if err != nil {
		panic(err)
	}
	// Don't forget to close the database.
	defer db.Close()
	const mapName = "employees"
	// Seed the random number generator.
	rand.Seed(time.Now().UnixNano())
	// Creating the mapping is required only once.
	if err := createMapping(db, mapName); err != nil {
		panic(err)
	}
	if err := populateMap(db, mapName, randomEmployees(10)); err != nil {
		panic(err)
	}
	emps, err := queryMap(db, mapName, 40)
	if err != nil {
		panic(err)
	}
	fmt.Println(emps)
}
