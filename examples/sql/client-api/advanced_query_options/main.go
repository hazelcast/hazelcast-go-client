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
	"errors"
	"fmt"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/sql"
)

func printStatement(stmt sql.Statement) {
	var (
		query            = stmt.SQL()
		params           = stmt.Parameters()
		timeout          = stmt.QueryTimeout()
		schema           = stmt.Schema()
		cursorBufferSize = stmt.CursorBufferSize()
	)
	var expectedResultType string
	switch stmt.ExpectedResultType() {
	case sql.ExpectedResultTypeAny:
		expectedResultType = "Any"
	case sql.ExpectedResultTypeRows:
		expectedResultType = "Rows"
	case sql.ExpectedResultTypeUpdateCount:
		expectedResultType = "Update Count"
	}
	fmt.Printf(`Stamement Details:
	Query String: %s
	Params: %+v
	Query Timeout: %d ms
	Schema: "%s"
	Expected Result Type: %s
	Cursor Buffer Size: %d`+"\n\n", query, params, timeout, schema, expectedResultType, cursorBufferSize)
}

func printResultDetails(r sql.Result) error {
	if r.IsRowSet() {
		rm, err := r.RowMetadata()
		if err != nil {
			return err
		}
		colCount := rm.ColumnCount()
		columns := rm.Columns()
		fmt.Printf("Row Result [%d columns]\n", colCount)
		for i, col := range columns {
			fmt.Printf(`	Column %d:
		Name: %s
		Type: %d
		Nullable: %t`+"\n", i, col.Name(), col.Type(), col.Nullable())
		}
	} else {
		fmt.Printf("Update Result [%d rows updated]\n", r.UpdateCount())
	}
	return nil
}

func executeAndPrintDetails(ctx context.Context, service sql.Service, stmt sql.Statement) (sql.Result, error) {
	printStatement(stmt)
	result, err := service.ExecuteStatement(ctx, stmt)
	if err != nil {
		var sqlError *sql.Error
		if errors.As(err, &sqlError) {
			err = fmt.Errorf(`SQLErr:
	Message: %s
	Suggestion: %s
	MemberID: %s
	ErrCode: %d`+"\n", sqlError.Message, sqlError.Suggestion, sqlError.OriginatingMemberId, sqlError.Code)
		}
		return nil, err
	}
	return result, printResultDetails(result)
}

func sinkValues(ctx context.Context, sqlService sql.Service, stmt sql.Statement) error {
	// set statement string and result type to reuse on iterations
	if err := stmt.SetSQL(`SINK INTO mymap VALUES(?, ?)`); err != nil {
		return fmt.Errorf("setting query string: %w", err)
	}
	// setting expected result type helps cluster to save time planning the statement execution
	if err := stmt.SetExpectedResultType(sql.ExpectedResultTypeUpdateCount); err != nil {
		return fmt.Errorf("setting result type: %w", err)
	}
	for i := 0; i < 100; i++ {
		// change params on every iteration
		stmt.SetParameters(i, fmt.Sprintf("sample-string-%d", i))
		result, err := executeAndPrintDetails(ctx, sqlService, stmt)
		if err != nil {
			return err
		}
		result.Close()
	}
	return nil
}

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
	r, err := executeAndPrintDetails(ctx, sqlService, stmt)
	if err != nil {
		panic(fmt.Errorf("creating the mapping: %w", err))
	}
	r.Close()
	if err = sinkValues(ctx, sqlService, stmt); err != nil {
		panic(fmt.Errorf("inserting values: %w", err))
	}
	// reset statement to reuse for following queries
	stmt.ClearParameters()
	stmt.SetQueryTimeout(5 * time.Second)
	if err := stmt.SetCursorBufferSize(10); err != nil {
		panic(fmt.Errorf("setting cursor buffer size: %w", err))
	}
	if err := stmt.SetSQL(`SELECT * from mymap order by __key`); err != nil {
		panic(fmt.Errorf("setting query string: %w", err))
	}
	result, err := executeAndPrintDetails(ctx, sqlService, stmt)
	if err != nil {
		panic(fmt.Errorf("query map: %w", err))
	}
	defer result.Close()
	it, err := result.Iterator()
	if err != nil {
		panic(fmt.Errorf("acquiring iterator: %w", err))
	}
	for it.HasNext() {
		row, err := it.Next()
		if err != nil {
			panic(fmt.Errorf("iterating rows: %w", err))
		}
		// we can access the value by column name
		key, err := row.GetByColumnName("__key")
		if err != nil {
			panic(fmt.Errorf("accessing row: %w", err))
		}
		// or we can access it via index
		index, err := row.Metadata().FindColumn("this")
		if err != nil {
			panic(fmt.Errorf("accessing row: %w", err))
		}
		value, err := row.Get(index)
		if err != nil {
			panic(fmt.Errorf("accessing row: %w", err))
		}
		fmt.Printf("Key %d --> %s\n", key, value)
	}
}
