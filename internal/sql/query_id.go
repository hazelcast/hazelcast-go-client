package sql

type QueryID struct {
	MemberIDHigh int64
	MemberIDLow  int64
	LocalIDHigh  int64
	LocalIDLow   int64
}
