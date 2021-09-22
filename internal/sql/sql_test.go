package sql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

func TestConnector_Driver(t *testing.T) {
	db, err := sql.Open("hazelcast", "localhost;Cluster.Unisocket=true")
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	rows, err := conn.QueryContext(ctx, "SELECT name FROM emp WHERE age < ?", 30)
	defer rows.Close()
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		if rows.Err() != nil {
			panic(rows.Err())
		}
		values, err := rows.Columns()
		if err != nil {
			panic(err)
		}
		fmt.Println(values)
	}
}
