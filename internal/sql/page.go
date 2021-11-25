package sql

import "database/sql/driver"

type Page struct {
	ColumnTypes []ColumnType
	Columns     [][]driver.Value
	Last        bool
}
