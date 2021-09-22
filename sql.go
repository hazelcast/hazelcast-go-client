package hazelcast

import "database/sql/driver"

type sqlRows struct {
	columnNames []string
	columns     []interface{}
}

func (s sqlRows) Columns() []string {
	return s.columnNames
}

func (s sqlRows) Close() error {
	return nil
}

func (s sqlRows) Next(dest []driver.Value) error {
	for i, col := range s.columns {
		dest[i] = col
	}
	return nil
}
