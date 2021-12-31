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

package driver_test

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/hazelcast/hazelcast-go-client/sql/driver"
)

func Example() {
	db, err := sql.Open("hazelcast", "hz://localhost:5701?cluster.name=pr-1234&cloud.token=123456789Aabcdef")
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	rows, err := conn.QueryContext(ctx, "SELECT name FROM emp WHERE age < ?", 30)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		if rows.Err() != nil {
			panic(rows.Err())
		}
		values, err := rows.Columns()
		if err != nil {
			panic(err)
		}
		fmt.Println(values)
	}
}
