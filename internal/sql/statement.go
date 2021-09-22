package sql

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
