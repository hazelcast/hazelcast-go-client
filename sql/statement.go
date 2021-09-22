package sql

import (
	"time"
)

type ExpectedResult int32

const (
	ExpectedResultAny         ExpectedResult = 0
	ExpectedResultRows        ExpectedResult = 1
	ExpectedResultUpdateCount ExpectedResult = 2
)

type Statement struct {
	SQL                string
	Schema             string
	Timeout            time.Duration
	CursorBufferSize   int
	ExpectedResultType ExpectedResult
	Params             []interface{}
}
