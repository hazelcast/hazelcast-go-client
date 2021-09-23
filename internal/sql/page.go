package sql

import "github.com/hazelcast/hazelcast-go-client/sql"

type Page struct {
	ColumnTypes []sql.ColumnType
	Columns     []interface{}
	Last        bool
}
