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

package main

import (
	"context"
	"fmt"
	"log"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/sql"
)

func main() {
	ctx := context.Background()
	c, err := hazelcast.StartNewClient(ctx)
	handleErr(err)
	tm, err := c.GetMap(ctx, "testMap")
	handleErr(err)
	err = tm.Clear(ctx)
	handleErr(err)
	sqlService := c.GetSQL()
	result, err := sqlService.ExecuteQuery(ctx,
		`CREATE OR REPLACE MAPPING "testMap"
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'varchar'
			)`)
	handleErr(err)
	fmt.Println(result.IsRowSet())
	fmt.Println(result.UpdateCount())
	result, err = sqlService.ExecuteQuery(ctx, `INSERT INTO "testMap" (__key, this) VALUES (?, ?),(?, ?)`,
		10, "someValue", 20, "otherValue")
	handleErr(err)
	fmt.Println(result.IsRowSet())
	fmt.Println(result.UpdateCount())
	stmt := sql.NewStatement(`SELECT * FROM "testMap"`)
	err = stmt.SetCursorBufferSize(1)
	handleErr(err)
	stmt.SetExpectedResultType(sql.ANY_RESULT)
	result, err = sqlService.Execute(ctx, stmt)
	handleErr(err)
	fmt.Println(result.IsRowSet())
	fmt.Println(result.UpdateCount())
	for result.HasNext() {
		row, err := result.Next()
		if err != nil {
			// handle error and finish iteration
			break
		}
		tmp, err := row.Get(0)
		handleErr(err)
		mapKey := tmp.(int64)
		tmp, err = row.Get(1)
		handleErr(err)
		mapValue := tmp.(string)
		fmt.Println(mapKey, mapValue)
		metadata := row.GetMetadata()
		firstColumn := metadata.GetColumns()[0]
		fmt.Println("Type of first column", firstColumn.GetType())
	}
}

func handleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
