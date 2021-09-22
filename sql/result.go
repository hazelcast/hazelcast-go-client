package sql

import (
	"context"
	"database/sql/driver"
)

type QueryResult struct {
	Row *Row
	Err error
}

func (r *QueryResult) Columns() []string {
	names := make([]string, len(r.Row.Metadata.Columns))
	for i := 0; i < len(names); i++ {
		names[i] = r.Row.Metadata.Columns[i].Name
	}
	return names
}

func (r *QueryResult) Close() error {
	return nil
}

func (r *QueryResult) Next(dest []driver.Value) error {
	cols := r.Row.Columns
	for i := 0; i < len(cols); i++ {
		dest[i] = cols[i]
	}
	return nil
}

func (r QueryResult) GetMetadata(ctx context.Context) (RowMetadata, error) {
	return r.Row.Metadata, nil
}

type ExecResult struct {
	UpdateCount int64
}

func (r ExecResult) LastInsertId() (int64, error) {
	return -1, nil
}

func (r ExecResult) RowsAffected() (int64, error) {
	return r.UpdateCount, nil
}

func (r ExecResult) GetUpdateCount(ctx context.Context) (int64, error) {
	panic("not implemented")
}
