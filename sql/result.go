package sql

import "context"

type Result struct {
	Row *Row
	Err error
}

func (r *Result) Next(ctx context.Context) bool {
	panic("not implemented")
}

func (r Result) GetUpdateCount(ctx context.Context) (int64, error) {
	panic("not implemented")
}

func (r Result) GetMetadata(ctx context.Context) (*RowMetadata, error) {
	panic("not implemented")
}

func (r *Result) Close(ctx context.Context) error {
	panic("not implemented")
}
