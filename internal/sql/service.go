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
	"sync/atomic"
	"unsafe"

	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	idriver "github.com/hazelcast/hazelcast-go-client/internal/sql/driver"
	"github.com/hazelcast/hazelcast-go-client/sql"
)

var (
	_ sql.Result       = &Result{}
	_ sql.RowsIterator = &Result{}
	_ sql.Row          = &Row{}
	_ sql.Service      = Service{}
)

type Service struct {
	service *idriver.SQLService
}

func NewService(cm *cluster.ConnectionManager, ss *iserialization.Service, cif *cluster.ConnectionInvocationFactory, is *invocation.Service, l *logger.LogAdaptor) Service {
	var s Service
	s.service = idriver.NewSQLService(cm, ss, cif, is, l)
	return s
}

// ExecuteStatement executes the given SQL statement.
func (s Service) ExecuteStatement(ctx context.Context, stmt sql.Statement) (sql.Result, error) {
	var err error
	if ctx, err = updateContextWithOptions(ctx, stmt); err != nil {
		return &Result{}, nil
	}
	stmtParams := stmt.Parameters()
	params := *(*[]driver.Value)(unsafe.Pointer(&stmtParams))
	resp, err := s.service.Execute(ctx, stmt.SQL(), params, stmt.ExpectedResultType())
	if err != nil {
		return &Result{}, err
	}
	var result Result
	switch r := resp.(type) {
	case *idriver.QueryResult:
		result.qr = r
	case *idriver.ExecResult:
		result.er = r
	default:
		return &Result{}, fmt.Errorf("unknown result type")
	}
	return &result, nil
}

// Execute is a convenient method to execute a distributed query with the given parameter values.
// You may define parameter placeholders in the query with the "?" character.
// For every placeholder, a value must be provided.
func (s Service) Execute(ctx context.Context, query string, params ...interface{}) (sql.Result, error) {
	return s.ExecuteStatement(ctx, sql.NewStatement(query, params...))
}

func updateContextWithOptions(ctx context.Context, opts sql.Statement) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, idriver.QueryCursorBufferSizeKey{}, opts.CursorBufferSize())
	ctx = context.WithValue(ctx, idriver.QueryTimeoutKey{}, opts.QueryTimeout())
	ctx = context.WithValue(ctx, idriver.QuerySchemaKey{}, opts.Schema())
	return ctx, nil
}

const (
	iteratorInitial   = 0
	iteratorRequested = 1
)

// Result implements sql.Result and sql.RowsIterator.
// Depending on the statement type it represents a stream of rows or an update count.
// It is not concurrency-safe except the Close and Iterator method.
type Result struct {
	qr            *idriver.QueryResult
	er            *idriver.ExecResult
	err           error
	currentRow    []driver.Value
	iteratorState int32
}

func (r *Result) Iterator() (sql.RowsIterator, error) {
	if !r.IsRowSet() {
		return nil, hzerrors.NewIllegalStateError("this result contains only update count", nil)
	}
	if !atomic.CompareAndSwapInt32(&r.iteratorState, iteratorInitial, iteratorRequested) {
		return nil, hzerrors.NewIllegalStateError("iterator can be requested only once", nil)
	}
	return r, nil
}

// RowMetadata returns metadata information about rows.
// An error is returned if result represents an update count.
func (r *Result) RowMetadata() (sql.RowMetadata, error) {
	if r.qr == nil {
		return nil, hzerrors.NewIllegalStateError("result contains only update count", nil)
	}
	return r.qr.Metadata(), nil
}

// IsRowSet returns whether this result has rows to iterate using the HasNext method.
func (r *Result) IsRowSet() bool {
	return r.qr != nil
}

// UpdateCount returns the number of rows updated by the statement or -1 if this result is a row set.
// In case the result doesn't contain rows but the update count isn't applicable or known, 0 is returned.
func (r *Result) UpdateCount() int64 {
	if r.er == nil {
		// means this is a query result
		return -1
	}
	return r.er.UpdateCount
}

// HasNext prepares the next result row for reading via Next method.
// It returns true on success, or false if there is no next result row or an error happened while preparing it.
func (r *Result) HasNext() bool {
	if r.err != nil {
		return false
	}
	row := make([]driver.Value, r.qr.Len())
	err := r.qr.Next(row)
	if err == io.EOF {
		return false
	}
	r.err = err
	r.currentRow = row
	return true
}

// Next returns the currentRow.
// Every call to Next, even the first one, must be preceded by a call to HasNext.
func (r *Result) Next() (sql.Row, error) {
	var row Row
	m, err := r.RowMetadata()
	if err != nil {
		return nil, err
	}
	r.qr.Metadata()
	row.metadata = m
	row.values = r.currentRow
	return row, r.err
}

// Err returns the error, if any, that was encountered during iteration.
// Err may be called after an explicit or implicit Close.
func (r *Result) Err() error {
	return r.err
}

// Close notifies the member to release resources for the corresponding query for results that represents a stream of rows.
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
	metadata sql.RowMetadata
	values   []driver.Value
}

// Get returns the value of the column by index. If index is out of range, an error is returned.
func (r Row) Get(index int) (interface{}, error) {
	if (index < 0) || (index >= len(r.values)) {
		return nil, fmt.Errorf("index out of range")
	}
	return r.values[index], nil
}

// GetByColumnName returns the value of the column by name. If columns does not exist, an error is returned.
func (r Row) GetByColumnName(colName string) (interface{}, error) {
	i, err := r.metadata.FindColumn(colName)
	if err != nil {
		return nil, hzerrors.NewIllegalArgumentError(fmt.Sprintf(`column "%s" doesn't exist`, colName), err)
	}
	return r.Get(i)
}

// Metadata returns the metadata information about the row.
func (r Row) Metadata() sql.RowMetadata {
	return r.metadata
}
