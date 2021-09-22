package sql

import (
	"database/sql/driver"

	pubsql "github.com/hazelcast/hazelcast-go-client/sql"
)

type Rows struct {
	row *pubsql.Row
}

func (r Rows) Columns() []string {
	names := make([]string, len(r.row.Metadata.Columns))
	for i := 0; i < len(names); i++ {
		names[i] = r.row.Metadata.Columns[i].Name
	}
	return names
}

func (r Rows) Close() error {
	return nil
}

func (r Rows) Next(dest []driver.Value) error {
	cols := r.Columns()
	for i := 0; i < len(cols); i++ {
		dest[i] = cols[i]
	}
	return nil
}
