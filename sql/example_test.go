package sql_test

import (
	"context"
	"database/sql"
)

/*
func ExampleResult() {
	ctx := context.Background()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		panic(err)
	}
	result, err := client.ExecuteSQL(ctx, "SELECT foo FROM bar where zoo=?", 38)
	if err != nil {
		panic(err)
	}
	defer result.Close(ctx)
	for result.Next(ctx) {
		if result.Err != nil {
			panic(result.Err)
		}
		fmt.Println(result.Row.Columns[0])
		// alternative: fmt.Println(result.Row.ColumnByIndex(0))
		// alternative: fmt.Println(result.Row.ColumnByName("foo"))
	}
}

*/

func ExampleDriver_Open() {
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
	/*
		rows, err := conn.QueryContext(ctx, "SELECT name FROM emp WHERE age < ?", 30)
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
	*/
}
