package main

import (
	"context"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"log"
)

/*
	In order to use CP Subsystem in, you need to have at least three member and CP should be enabled in the XML.
	Zero member means CP Subsystem is disabled.
	<cp-subsystem>
		<cp-member-count>3</cp-member-count>
		<group-size>3</group-size>
	</cp-subsystem>
*/

func main() {
	ctx := context.Background()
	client, err := hazelcast.StartNewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	cp := client.CPSubsystem()
	viewCounter, err := cp.GetAtomicLong(context.Background(), "views")
	if err != nil {
		log.Fatal(err)
	}
	val, err := viewCounter.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(val)
	err = viewCounter.Set(ctx, 10)
	if err != nil {
		log.Fatal(err)
	}
	val, err = viewCounter.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(val)
	val, err = viewCounter.AddAndGet(ctx, 50)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(val)
	val, err = viewCounter.IncrementAndGet(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(val)
	e, err := viewCounter.CompareAndSet(ctx, 61, 62)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(e)
	val, err = viewCounter.DecrementAndGet(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(val)
}
