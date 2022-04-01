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
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/sql"
)

func main() {
	ctx := context.Background()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		panic(fmt.Errorf("creating the client"))
	}
	defer client.Shutdown(ctx)
	stmt := sql.NewStatement(`
			CREATE OR REPLACE MAPPING mymap
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'varchar'
			)
	`)
	stmt.SetSchema("partitioned")
	sqlService := client.SQL()
	_, err = sqlService.ExecuteStatement(ctx, stmt)
	if err != nil {
		panic(fmt.Errorf("creating the mapping: %w", err))
	}
	for i := 0; i < 100; i++ {
		if _, err = sqlService.Execute(ctx, `SINK INTO mymap VALUES(?, ?)`,
			i, fmt.Sprintf("sample string-%d", i)); err != nil {
			panic(fmt.Errorf("inserting values: %w", err))
		}
	}
	stmt = sql.NewStatement(`SELECT __key, this from mymap order by __key`)
	stmt.SetQueryTimeout(5 * time.Second)
	stmt.SetCursorBufferSize(10)
	result, err := sqlService.ExecuteStatement(ctx, stmt)
	if err != nil {
		panic(fmt.Errorf("querying: %w", err))
	}
	defer result.Close()
	it, err := result.Iterator()
	if err != nil {
		panic(fmt.Errorf("acquiring iterator: %w", err))
	}
	var (
		k   int64
		v   string
		tmp interface{}
	)
	for it.HasNext() {
		row, err := it.Next()
		if err != nil {
			panic(fmt.Errorf("iterating rows: %w", err))
		}
		tmp, err = row.Get(0)
		if err != nil {
			panic(fmt.Errorf("accessing row: %w", err))
		}
		k = tmp.(int64)
		tmp, err = row.Get(1)
		if err != nil {
			panic(fmt.Errorf("accessing row: %w", err))
		}
		v = tmp.(string)
		fmt.Printf("--> %d: %s\n", k, v)
	}
}
