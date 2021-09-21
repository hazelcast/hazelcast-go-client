package sql

import (
	"context"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
)

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
