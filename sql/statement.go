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
	"fmt"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/check"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

// duplication of constants from internal driver package.
const (
	defaultCursorBufferSize int32 = 4096
	defaultTimeoutMillis    int64 = -1
	defaultSchema                 = ""
)

/*
Statement represents an SQL Statement. Use NewStatement to benefit
from the default settings.
Fields are read once before the execution is started.
Changes to fields do not affect the behavior of already running statements.
*/
type Statement struct {
	SQL                string
	schema             string
	Parameters         []interface{}
	timeout            int64
	cursorBufferSize   int32
	expectedResultType ExpectedResultType
}

// NewStatement returns a new sql Statement with provided arguments
// You may define parameter placeholders in the Statement with the "?" character.
// For every placeholder, a value must be provided in 'params'.
func NewStatement(statement string, params ...interface{}) Statement {
	return Statement{
		SQL:                statement,
		Parameters:         params,
		cursorBufferSize:   defaultCursorBufferSize,
		timeout:            defaultTimeoutMillis,
		schema:             defaultSchema,
		expectedResultType: AnyResult,
	}
}

/*
SetCursorBufferSize sets the query cursor buffer size.
When rows are ready to be consumed, they are put into an internal buffer of the cursor.
This parameter defines the maximum number of rows in that buffer.
When the threshold is reached, the backpressure mechanism will slow down the execution, possibly to a complete halt, to prevent out-of-memory.
The default value is expected to work well for most workloads.
A bigger buffer size may give you a slight performance boost for queries with large result sets at the cost of increased memory consumption.
Defaults to 4096.
The given buffer size must be in the non-negative int32 range.
*/
func (s *Statement) SetCursorBufferSize(cbs int) error {
	v, err := check.NonNegativeInt32(cbs)
	if err != nil {
		return ihzerrors.NewIllegalArgumentError("setting cursor buffer size", err)
	}
	s.cursorBufferSize = v
	return nil
}

/*
SetQueryTimeout sets the query execution timeout.
If the timeout is reached for a running Statement, it will be cancelled forcefully.
Zero value means no timeout.
Negative values mean that the value from the server-side config will be used.
Defaults to -1.
*/
func (s *Statement) SetQueryTimeout(t time.Duration) {
	tm := t.Milliseconds()
	// note that the condition below is for t, not tm
	if t < 0 {
		tm = -1
	}
	s.timeout = tm
}

/*
SetSchema sets the schema name.
The engine will try to resolve the non-qualified object identifiers from the Statement in the given schema.
If not found, the default search path will be used.
The schema name is case-sensitive. For example, foo and Foo are different schemas.
By default, only the default search path is used, which looks for objects in the predefined schemas "partitioned" and "public".
*/
func (s *Statement) SetSchema(schema string) {
	s.schema = schema
}

// SetExpectedResultType sets the expected result type.
// Returns an error if parameter is not one of AnyResult, RowsResult or UpdateCountResult.
func (s *Statement) SetExpectedResultType(resultType ExpectedResultType) error {
	switch resultType {
	case AnyResult, RowsResult, UpdateCountResult:
		s.expectedResultType = resultType
		return nil
	default:
		return fmt.Errorf("invalid result type parameter")
	}
}

// CursorBufferSize returns the cursor buffer size (measured in the number of rows).
func (s *Statement) CursorBufferSize() int32 {
	return s.cursorBufferSize
}

// QueryTimeout returns the execution timeout in milliseconds.
// -1 means timeout is not set and member configuration SqlConfig#setStatementTimeoutMillis will be respected.
func (s Statement) QueryTimeout() int64 {
	return s.timeout
}

// Schema returns the schema name.
func (s *Statement) Schema() string {
	return s.schema
}

// ExpectedResultType returns ExpectedResultType.
func (s *Statement) ExpectedResultType() ExpectedResultType {
	return s.expectedResultType
}
