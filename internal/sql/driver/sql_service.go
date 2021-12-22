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
	"fmt"
	"math"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	isql "github.com/hazelcast/hazelcast-go-client/internal/sql"
)

const (
	expectedResultAny         byte = 0
	expectedResultRows        byte = 1
	expectedResultUpdateCount byte = 2
)

type SQLService struct {
	connectionManager    *cluster.ConnectionManager
	serializationService *iserialization.Service
	invFactory           *cluster.ConnectionInvocationFactory
	invService           *invocation.Service
	cb                   *cb.CircuitBreaker
}

func newSQLService(cm *cluster.ConnectionManager, ss *iserialization.Service, fac *cluster.ConnectionInvocationFactory, is *invocation.Service) *SQLService {
	cbr := cb.NewCircuitBreaker(
		cb.MaxRetries(math.MaxInt32),
		cb.RetryPolicy(func(attempt int) time.Duration {
			return time.Duration((attempt+1)*100) * time.Millisecond
		}),
	)
	return &SQLService{
		connectionManager:    cm,
		serializationService: ss,
		invFactory:           fac,
		invService:           is,
		cb:                   cbr,
	}
}

// ExecuteSQL runs the given SQL query on the member-side.
// Placeholders in the query is replaced by params.
// A placeholder is the question mark (?) character.
// For each placeholder, a corresponding param should exist.
func (s *SQLService) ExecuteSQL(ctx context.Context, query string, params []driver.Value) (*ExecResult, error) {
	cbs := ExtractCursorBufferSize(ctx)
	tom := ExtractTimeoutMillis(ctx)
	resp, err := s.executeSQL(ctx, query, expectedResultUpdateCount, tom, cbs, params)
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
	resp, err := s.executeSQL(ctx, query, expectedResultRows, tom, cbs, params)
	if err != nil {
		return nil, err
	}
	return resp.(*QueryResult), nil
}

func (s *SQLService) Fetch(qid isql.QueryID, conn *cluster.Connection, cbs int32) (*isql.Page, error) {
	req := codec.EncodeSqlFetchRequest(qid, cbs)
	resp, err := s.invokeOnConnection(context.Background(), req, conn)
	if err != nil {
		return nil, err
	}
	page, err := codec.DecodeSqlFetchResponse(resp, s.serializationService)
	if err != (*isql.Error)(nil) {
		return nil, err
	}
	return page, nil
}

func (s *SQLService) CloseQuery(qid isql.QueryID, conn *cluster.Connection) error {
	req := codec.EncodeSqlCloseRequest(qid)
	_, err := s.invokeOnConnection(context.Background(), req, conn)
	if err != nil {
		return fmt.Errorf("closing query: %w", err)
	}
	return nil
}

func (s *SQLService) executeSQL(ctx context.Context, query string, resultType byte, timeoutMillis int64, cursorBufferSize int32, params []driver.Value) (interface{}, error) {
	serParams, err := s.serializeParams(params)
	if err != nil {
		return nil, err
	}
	conn := s.connectionManager.RandomConnection()
	if conn == nil {
		return nil, ihzerrors.NewIOError("no connection found", nil)
	}
	qid := isql.NewQueryIDFromUUID(conn.MemberUUID())
	req := codec.EncodeSqlExecuteRequest(query, serParams, timeoutMillis, cursorBufferSize, "", resultType, qid, false)
	resp, err := s.invokeOnConnection(ctx, req, conn)
	if err != nil {
		return nil, err
	}
	metadata, page, updateCount, err := codec.DecodeSqlExecuteResponse(resp, s.serializationService)
	if err != (*isql.Error)(nil) {
		return nil, err
	}
	if updateCount >= 0 {
		return &ExecResult{UpdateCount: updateCount}, nil
	}
	md := isql.RowMetadata{Columns: metadata}
	return NewQueryResult(qid, md, page, s, conn, cursorBufferSize)
}

func (s *SQLService) serializeParams(params []driver.Value) ([]*iserialization.Data, error) {
	serParams := make([]*iserialization.Data, len(params))
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
	now := time.Now()
	return s.tryInvoke(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
		if attempt > 0 {
			req = req.Copy()
		}
		inv := s.invFactory.NewConnectionBoundInvocation(req, conn, nil, now)
		if err := s.invService.SendRequest(ctx, inv); err != nil {
			return nil, err
		}
		return inv.GetWithContext(ctx)
	})
}

func (s *SQLService) tryInvoke(ctx context.Context, f cb.TryHandler) (*proto.ClientMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	res, err := s.cb.TryContext(ctx, f)
	if err != nil {
		return nil, err
	}
	return res.(*proto.ClientMessage), nil
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
