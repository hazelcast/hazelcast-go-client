package sql

type ColumnType int32

const (
	ColumnTypeVarchar               ColumnType = 0
	ColumnTypeBoolean               ColumnType = 1
	ColumnTypeTinyInt               ColumnType = 2
	ColumnTypeSmallInt              ColumnType = 3
	ColumnTypeInt                   ColumnType = 4
	ColumnTypeBigInt                ColumnType = 5
	ColumnTypeDecimal               ColumnType = 6
	ColumnTypeReal                  ColumnType = 7
	ColumnTypeDouble                ColumnType = 8
	ColumnTypeDate                  ColumnType = 9
	ColumnTypeTime                  ColumnType = 10
	ColumnTypeTimestamp             ColumnType = 11
	ColumnTypeTimestampWithTimeZone ColumnType = 12
	ColumnTypeObject                ColumnType = 13
	ColumnTypeNull                  ColumnType = 14
)

type ColumnMetadata struct {
	Name     string
	Type     ColumnType
	Nullable bool
}
