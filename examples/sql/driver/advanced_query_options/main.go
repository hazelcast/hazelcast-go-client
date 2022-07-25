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

	"github.com/hazelcast/hazelcast-go-client/sql/driver"

	"github.com/hazelcast/hazelcast-go-client"
)

func main() {
	config := hazelcast.Config{}
	db := driver.Open(config)
	defer db.Close()
	ctx := driver.WithSchema(context.Background(), "partitioned")
	_, err := db.ExecContext(ctx, `
			CREATE OR REPLACE MAPPING mymap
			TYPE IMAP
			OPTIONS (
				'keyFormat' = 'bigint',
				'valueFormat' = 'varchar'
			)
	`)
	if err != nil {
		panic(fmt.Errorf("creating the mapping: %w", err))
	}
	stmt, err := db.Prepare(`SINK INTO mymap VALUES(?, ?)`)
	if err != nil {
		panic(fmt.Errorf("preparing statement: %w", err))
	}
	defer stmt.Close()
	for i := 0; i < 100; i++ {
		if _, err = stmt.ExecContext(ctx, i, fmt.Sprintf("sample string-%d", i)); err != nil {
			panic(fmt.Errorf("inserting values: %w", err))
		}
	}
	ctx = driver.WithQueryTimeout(ctx, 5*time.Second)
	ctx = driver.WithCursorBufferSize(ctx, 10)
	rows, err := db.QueryContext(ctx, `SELECT __key, this from mymap order by __key`)
	if err != nil {
		panic(fmt.Errorf("querying: %w", err))
	}
	defer rows.Close()
	var k int64
	var v string
	for rows.Next() {
		if err := rows.Scan(&k, &v); err != nil {
			panic(fmt.Errorf("scanning: %w", err))
		}
		fmt.Printf("--> %d: %s\n", k, v)
	}
}
