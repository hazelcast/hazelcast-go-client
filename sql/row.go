package sql

type Row struct {
	Metadata RowMetadata
	Columns  []interface{}
}

func (r Row) ColumnByName(name string) interface{} {
	panic("not implemented")
}

func (r Row) ColumnByIndex(index int) interface{} {
	panic("not implemented")
}
