/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/internal/client"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	itypes "github.com/hazelcast/hazelcast-go-client/internal/sql/types"
	"github.com/hazelcast/hazelcast-go-client/sql"
)

const (
	ExpectedResultAny         byte = 0
	ExpectedResultRows        byte = 1
	ExpectedResultUpdateCount byte = 2
)

type SQLService struct {
	connectionManager    *cluster.ConnectionManager
	serializationService *iserialization.Service
	lg                   *logger.LogAdaptor
	invoker              *client.Invoker
}

func NewSQLService(cm *cluster.ConnectionManager, ss *iserialization.Service, invoker *client.Invoker, lg *logger.LogAdaptor) *SQLService {
	return &SQLService{
		connectionManager:    cm,
		serializationService: ss,
		invoker:              invoker,
		lg:                   lg,
	}
}

func (s *SQLService) Execute(ctx context.Context, query string, params []driver.Value, resultType sql.ExpectedResultType) (interface{}, error) {
	cbs := ExtractCursorBufferSize(ctx)
	tom := ExtractTimeoutMillis(ctx)
	schema := ExtractSchema(ctx)
	return s.executeSQL(ctx, query, byte(resultType), tom, cbs, schema, params)
}

// ExecuteSQL runs the given SQL query on the member-side.
// Placeholders in the query is replaced by params.
// A placeholder is the question mark (?) character.
// For each placeholder, a corresponding param should exist.
func (s *SQLService) ExecuteSQL(ctx context.Context, query string, params []driver.Value) (*ExecResult, error) {
	cbs := ExtractCursorBufferSize(ctx)
	tom := ExtractTimeoutMillis(ctx)
	schema := ExtractSchema(ctx)
	resp, err := s.executeSQL(ctx, query, ExpectedResultUpdateCount, tom, cbs, schema, params)
	if err != nil {
		return nil, err
	}
	return resp.(*ExecResult), nil
}

// QuerySQL runs the given SQL query on the member-side.
// Placeholders in the query is replaced by params.
// A placeholder is the question mark (?) character.
// For each placeholder, a corresponding parameter should exist.
func (s *SQLService) QuerySQL(ctx context.Context, query string, params []driver.Value) (*QueryResult, error) {
	cbs := ExtractCursorBufferSize(ctx)
	tom := ExtractTimeoutMillis(ctx)
	schema := ExtractSchema(ctx)
	resp, err := s.executeSQL(ctx, query, ExpectedResultRows, tom, cbs, schema, params)
	if err != nil {
		return nil, err
	}
	return resp.(*QueryResult), nil
}

func (s *SQLService) fetch(ctx context.Context, qid itypes.QueryID, conn *cluster.Connection, cbs int32) (itypes.Page, error) {
	req := codec.EncodeSqlFetchRequest(qid, cbs)
	resp, err := s.invokeOnConnection(ctx, req, conn)
	if err != nil {
		return itypes.Page{}, err
	}
	page, err := codec.DecodeSqlFetchResponse(resp, s.serializationService)
	if err != (*sql.Error)(nil) {
		return itypes.Page{}, ihzerrors.NewSQLError("decoding SQL fetch response", err)
	}
	if page == nil {
		return itypes.Page{}, nil
	}
	return *page, nil
}

func (s *SQLService) closeQuery(ctx context.Context, qid itypes.QueryID, conn *cluster.Connection) error {
	req := codec.EncodeSqlCloseRequest(qid)
	if _, err := s.invokeOnConnection(ctx, req, conn); err != nil {
		return fmt.Errorf("closing query: %w", err)
	}
	return nil
}

func (s *SQLService) executeSQL(ctx context.Context, query string, resultType byte, timeoutMillis int64, cursorBufferSize int32, schema string, params []driver.Value) (interface{}, error) {
	serParams, err := s.serializeParams(params)
	if err != nil {
		return nil, err
	}
	conn := s.connectionManager.SQLConnection()
	if conn == nil {
		return nil, ihzerrors.NewIOError("no connection found", nil)
	}
	qid := itypes.NewQueryIDFromUUID(conn.MemberUUID())
	req := codec.EncodeSqlExecuteRequest(query, serParams, timeoutMillis, cursorBufferSize, schema, resultType, qid, false)
	s.lg.Debug(func() string {
		return fmt.Sprintf("SqlExecuteRequest: qid: %d, q: %s", qid, query)
	})
	resp, err := s.invokeOnConnection(ctx, req, conn)
	if err != nil {
		return nil, err
	}
	metadata, page, updateCount, infiniteRows, err := codec.DecodeSqlExecuteResponse(resp, s.serializationService)
	if err != (*sql.Error)(nil) {
		return nil, ihzerrors.NewSQLError("decoding SQL execute response", err)
	}
	if updateCount >= 0 {
		return &ExecResult{UpdateCount: updateCount}, nil
	}
	md := itypes.NewRowMetadata(metadata)
	var p itypes.Page
	if page != nil {
		p = *page
	}
	return NewQueryResult(ctx, qid, md, p, s, conn, cursorBufferSize, infiniteRows)
}

func (s *SQLService) serializeParams(params []driver.Value) ([]iserialization.Data, error) {
	serParams := make([]iserialization.Data, len(params))
	for i, param := range params {
		data, err := s.serializationService.ToData(param)
		if err != nil {
			return nil, err
		}
		serParams[i] = data
	}
	return serParams, nil
}

func (s *SQLService) invokeOnConnection(ctx context.Context, req *proto.ClientMessage, conn *cluster.Connection) (*proto.ClientMessage, error) {
	return s.invoker.InvokeOnConnection(ctx, req, conn)
}

func ExtractCursorBufferSize(ctx context.Context) int32 {
	if ctx == nil {
		return DefaultCursorBufferSize
	}
	cbsv := ctx.Value(QueryCursorBufferSizeKey{})
	if cbsv == nil {
		return DefaultCursorBufferSize
	}
	return cbsv.(int32)
}

func ExtractTimeoutMillis(ctx context.Context) int64 {
	if ctx == nil {
		return DefaultTimeoutMillis
	}
	tomv := ctx.Value(QueryTimeoutKey{})
	if tomv == nil {
		return DefaultTimeoutMillis
	}
	return tomv.(int64)
}

func ExtractSchema(ctx context.Context) string {
	if ctx == nil {
		return DefaultSchema
	}
	v := ctx.Value(QuerySchemaKey{})
	if v == nil {
		return DefaultSchema
	}
	return v.(string)
}
