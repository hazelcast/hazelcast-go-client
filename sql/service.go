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

package sql

import (
	"context"
)

// Service represents the SQL service.
type Service interface {
	/*
		ExecuteStatement executes the given SQL Statement.
		Example:

			stmt := NewStatement(`INSERT INTO person(__key, age, name) VALUES (?, ?, ?)`, 1001, 35, "Jane Doe")
			client, err := hazelcast.StartNewClient(context.TODO())
			// handle the error
			sqlService := client.GetSQL()
			result, err := sqlService.ExecuteStatement(context.TODO(), stmt)
			// handle the error
			cnt, err := result.UpdateCount()
			// handle the error
			fmt.Printf("Affected rows: %d\n", cnt)

		Example:

			client, err := hazelcast.StartNewClient(context.TODO())
				// handle the error
				sqlService := client.GetSQL()
				stmt := sql.NewStatement(`SELECT name, age FROM person WHERE age >= ?`, 30)
				stmt.SetCursorBufferSize(1000)
				result, err := sqlService.ExecuteStatement(context.TODO(), stmt)
				// handle the error
				defer result.Close()
				var name string
				var age int
				for result.HasNext() {
					row, err := result.Next()
					// handle the error
					v1, err := row.Get(0)
					// handle the error
					v2, err := row.Get(1)
					// handle the error
					name, age = v1.(string), v2.(int)
					fmt.Println(name, age)
				}
			}
	*/
	ExecuteStatement(ctx context.Context, stmt Statement) (Result, error)
	/*
		Execute is a convenience method to execute a distributed query with the given parameter
		values. You may define parameter placeholders in the query with the "?" character.
		For every placeholder, a value must be provided.
		Example:

			client, err := hazelcast.StartNewClient(context.TODO())
			// handle the error
			sqlService := client.GetSQL()
			result, err := client.Execute(context.TODO(), `INSERT INTO person(__key, age, name) VALUES (?, ?, ?)`, 1001, 35, "Jane Doe")
			// handle the error
			cnt, err := result.UpdateCount()
			// handle the error
			fmt.Printf("Affected rows: %d\n", cnt)
	*/
	Execute(ctx context.Context, query string, params ...interface{}) (Result, error)
}
