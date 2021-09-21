package sql

type ColumnType int32

type ColumnMetadata struct {
	Name     string
	Type     ColumnType
	Nullable bool
}

type RowMetadata struct {
	Columns []ColumnMetadata
}

func (m RowMetadata) ColumnByName(name string) ColumnMetadata {
	panic("not implemented")
}
