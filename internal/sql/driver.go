package sql

import (
	"database/sql"
	"database/sql/driver"
)

const (
	driverName = "hazelcast"
)

var (
	_ driver.Driver = (*Driver)(nil)
)

type Driver struct {
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	return newConn(name)
}

func init() {
	sql.Register(driverName, &Driver{})
}
