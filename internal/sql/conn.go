package sql

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
)

var (
	_ driver.Conn = (*Conn)(nil)
)

type Conn struct {
	client *hazelcast.Client
}

func newConn(name string) (*Conn, error) {
	config := hazelcast.Config{}
	client, err := hazelcast.StartNewClientWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("starting Hazelcast client: %w", err)
	}
	return &Conn{client: client}, nil
}

func (c Conn) Prepare(query string) (driver.Stmt, error) {
	return newStatement(query, c.client), nil
}

func (c *Conn) Close() error {
	return c.client.Shutdown(context.Background())
}

func (c Conn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}
