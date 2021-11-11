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

package driver

import (
	"context"
	"database/sql/driver"
	"io"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client"
	pubsql "github.com/hazelcast/hazelcast-go-client/sql"
)

type Statement struct {
	stmt    pubsql.Statement
	client  *hazelcast.Client
	counter int32
}

func newStatement(sql string, client *hazelcast.Client) *Statement {
	return &Statement{
		stmt:   pubsql.Statement{SQL: sql},
		client: client,
	}
}

func (s Statement) Close() error {
	return nil
}

func (s Statement) NumInput() int {
	return -1
}

func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	return s.client.ExecuteSQL(context.Background(), s.stmt.SQL, args)
}

func (s Statement) Query(args []driver.Value) (driver.Rows, error) {
	if atomic.AddInt32(&s.counter, 1)-1 > 3 {
		return nil, io.EOF
	}
	return s.client.QuerySQL(context.Background(), s.stmt.SQL, args)
}
