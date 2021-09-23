package sql

type ColumnType int32

const (
	ColumnTypeVarchar ColumnType = 0
	ColumnTypeBoolean ColumnType = 1
	ColumnTypeTinyInt ColumnType = 2
)

type ColumnMetadata struct {
	Name     string
	Type     ColumnType
	Nullable bool
}
