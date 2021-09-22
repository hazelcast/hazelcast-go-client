package sql

import (
	"context"
	"database/sql/driver"
)

type Connector struct {
}

func (c Connector) Connect(ctx context.Context) (driver.Conn, error) {
	panic("implement me")
}

func (c Connector) Driver() driver.Driver {
	panic("implement me")
}
