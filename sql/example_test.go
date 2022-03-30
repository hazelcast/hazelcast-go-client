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

package sql_test

import (
	"context"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/sql"
)

func ExampleService_ExecuteStatement() {
	stmt := sql.NewStatement(`INSERT INTO person(__key, age, name) VALUES (?, ?, ?)`, 1001, 35, "Jane Doe")
	client, err := hazelcast.StartNewClient(context.TODO())
	if err != nil {
		// handle the error
	}
	sqlService := client.GetSQL()
	result, err := sqlService.ExecuteStatement(context.TODO(), stmt)
	if err != nil {
		// handle the error
	}
	cnt := result.UpdateCount()
	fmt.Printf("Affected rows: %d\n", cnt)
}

func Example() {
	client, err := hazelcast.StartNewClient(context.TODO())
	if err != nil {
		// handle the error
	}
	sqlService := client.GetSQL()
	stmt := sql.NewStatement(`SELECT name, age FROM person WHERE age >= ?`, 30)
	err = stmt.SetCursorBufferSize(1000)
	if err != nil {
		// handle the error
	}
	result, err := sqlService.ExecuteStatement(context.TODO(), stmt)
	if err != nil {
		// handle the error
	}
	defer result.Close()
	it, err := result.Iterator()
	if err != nil {
		// handle the error
	}
	var name string
	var age int
	for it.HasNext() {
		row, err := it.Next()
		if err != nil {
			// handle the error
		}
		v1, err := row.Get(0)
		if err != nil {
			// handle the error
		}
		v2, err := row.Get(1)
		if err != nil {
			// handle the error
		}
		name, age = v1.(string), v2.(int)
		fmt.Println(name, age)
	}
}
