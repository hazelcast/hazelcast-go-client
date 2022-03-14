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
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	idriver "github.com/hazelcast/hazelcast-go-client/internal/sql/driver"
	"github.com/hazelcast/hazelcast-go-client/internal/sql/types"
	hzsql "github.com/hazelcast/hazelcast-go-client/sql"
)

type Service struct {
	iService *idriver.SQLService
}

func New(cm *cluster.ConnectionManager, ss *iserialization.Service, cif *cluster.ConnectionInvocationFactory, is *invocation.Service, l *logger.LogAdaptor) Service {
	var s Service
	s.iService = idriver.NewSQLService(cm, ss, cif, is, l)
	return s
}

// Execute executes the given SQL statement.
func (s Service) Execute(ctx context.Context, stmt hzsql.Statement) (Result, error) {
	var err error
	if ctx, err = updateContextWithOptions(ctx, stmt); err != nil {
		return Result{}, nil
	}
	var sqlParams []driver.Value
	for _, p := range stmt.Args {
		sqlParams = append(sqlParams, p)
	}
	resp, err := s.iService.Execute(ctx, stmt.SQL, sqlParams)
	if err != nil {
		return Result{}, err
	}
	var result Result
	switch r := resp.(type) {
	case *idriver.QueryResult:
		result.qr = r
	case *idriver.ExecResult:
		result.er = r
	default:
		// todo return err
	}
	return result, nil
}

func updateContextWithOptions(ctx context.Context, opts hzsql.Statement) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, idriver.QueryCursorBufferSizeKey{}, opts.CursorBufferSize())
	ctx = context.WithValue(ctx, idriver.QueryTimeoutKey{}, opts.QueryTimeout())
	ctx = context.WithValue(ctx, idriver.QuerySchemaKey{}, opts.Schema())
	return ctx, nil
}

// Result represents a query result. Depending on the statement type it represents a stream of rows or an update count.
type Result struct {
	qr         *idriver.QueryResult
	er         *idriver.ExecResult
	err        error
	currentRow []driver.Value
}

// RowMetadata returns metadata information about rows. An error is returned if result represents an update count.
func (r *Result) RowMetadata() (types.RowMetadata, error) {
	if r.qr == nil {
		return types.RowMetadata{}, fmt.Errorf("result contains only update count")
	}
	metadata := r.qr.Metadata()
	metadata.Columns = metadata.Columns[:] // not sure if we should copy
	return metadata, nil
}

// IsRowSet returns whether this result has rows to iterate using the Next method.
func (r *Result) IsRowSet() bool {
	return r.UpdateCount() == -1
}

// UpdateCount returns the number of rows updated by the statement or -1 if this result
// is a row set. In case the result doesn't contain rows but the update
// count isn't applicable or known, 0 is returned.
func (r *Result) UpdateCount() int64 {
	if r.er == nil {
		// means this is a query result
		return -1
	}
	return r.er.UpdateCount
}

// Next prepares the next result row for reading with the Value method. It
// returns true on success, or false if there is no next result row or an error
// happened while preparing it. Err should be consulted to distinguish between
// the two cases.
//
// Every call to Value, even the first one, must be preceded by a call to Next.
func (r *Result) Next() bool {
	emptyValues := make([]driver.Value, r.qr.Len(), r.qr.Len())
	err := r.qr.Next(emptyValues)
	if err == nil {
		r.currentRow = emptyValues
		return true
	}
	if err != io.EOF {
		r.err = err
	}
	return false
}

// Value returns the currentRow.
func (r *Result) Value() Row {
	var row Row
	m, _ := r.RowMetadata()
	row.metadata = m
	row.values = r.currentRow
	return row
}

// Err returns the error, if any, that was encountered during iteration.
// Err may be called after an explicit or implicit Close.
func (r *Result) Err() error {
	return r.err
}

// Close , for results that represents a stream of rows, notifies the member to release resources for the corresponding query.
// It can be safely called more than once, and it is concurrency-safe.
// If result represents an update count, it has no effect.
func (r *Result) Close() error {
	if r.qr != nil {
		return r.qr.Close()
	}
	return nil
}

// Row represents an SQL result row.
type Row struct {
	metadata types.RowMetadata
	values   []driver.Value
}

// Get returns the value of the column by index. If index is out of range, an error is returned.
func (r *Row) Get(index int) (interface{}, error) {
	if (index < 0) || (index >= len(r.values)) {
		return nil, fmt.Errorf("index out of range")
	}
	return r.values[index], nil
}

// GetFromColumn returns the value of the column by name. If columns does not exist, an error is returned.
func (r *Row) GetFromColumn(colName string) (interface{}, error) {
	panic("implement me")
}

// GetMetadata returns the metadata information about the row.
func (r *Row) GetMetadata() types.RowMetadata {
	return r.metadata
}
