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
	"sort"
)

type Statement struct {
	ss    *SQLService
	query string
}

func newStatement(query string, ss *SQLService) *Statement {
	return &Statement{
		query: query,
		ss:    ss,
	}
}

func (s Statement) Close() error {
	return nil
}

func (s Statement) NumInput() int {
	return -1
}

func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	return s.ss.ExecuteSQL(context.Background(), s.query, args)
}

func (s Statement) Query(args []driver.Value) (driver.Rows, error) {
	return s.ss.QuerySQL(context.Background(), s.query, args)
}

func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.ss.ExecuteSQL(ctx, s.query, namedValuesToValues(args))
}

func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.ss.QuerySQL(ctx, s.query, namedValuesToValues(args))
}

func namedValuesToValues(nArgs []driver.NamedValue) []driver.Value {
	var args []driver.Value
	if len(nArgs) > 0 {
		sort.Slice(nArgs, func(i, j int) bool {
			return nArgs[i].Ordinal < nArgs[j].Ordinal
		})
		args = make([]driver.Value, len(nArgs))
		for i, arg := range nArgs {
			args[i] = arg.Value
		}
	}
	return args
}
