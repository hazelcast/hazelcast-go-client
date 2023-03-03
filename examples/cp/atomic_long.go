package main

import (
	"context"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

/*
To use CP Subsystem, you need to have at least three member in your cluster.
Member count which will be used by CP Subsystem has to be specified in the Hazelcast config file.
Default member count is 0 which disables the CP Subsystem.

	<cp-subsystem>
		<cp-member-count>3</cp-member-count>
		<group-size>3</group-size>
	</cp-subsystem>
*/

func main() {
	ctx := context.Background()
	cfg := hazelcast.Config{}
	cfg.Serialization.SetIdentifiedDataSerializableFactories(&MultiplicationFactory{})
	client, err := hazelcast.StartNewClientWithConfig(ctx, cfg)
	if err != nil {
		panic(err)
	}
	cp := client.CPSubsystem()
	counter, err := cp.GetAtomicLong(ctx, "counter")
	val, err := counter.Get(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("counter after Get:", val)
	err = counter.Set(ctx, 10)
	if err != nil {
		panic(err)
	}
	val, err = counter.Get(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("counter after Set:", val)
	val, err = counter.AddAndGet(ctx, 50)
	if err != nil {
		panic(err)
	}
	fmt.Println("counter after AddAndGet:", val)
	val, err = counter.IncrementAndGet(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("counter after IncrementAndGet:", val)
	res, err := counter.CompareAndSet(ctx, 61, 62)
	if err != nil {
		panic(err)
	}
	fmt.Println("CompareAndSet operation result:", res)
	val, err = counter.DecrementAndGet(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("counter after DecrementAndGet:", val)
	val, err = counter.AlterAndGet(ctx, &Multiplication{2})
	if err != nil {
		panic(err)
	}
	fmt.Println("counter after AlterAndGet:", val)
	modified, err := counter.Apply(ctx, &Multiplication{3})
	fmt.Println(": ", modified)
}

/*
Multiplication is a function for multiplying the data by given multiplier.
It must have a counterpart registered in the server-side that implements the “com.hazelcast.core.IFunction“ interface.
Identified serialization method is used for serializing the function to call it on Alter() and Apply() methods.
*/
const multiplicationFactoryID = 66
const multiplicationProcessorClassID = 16

type Multiplication struct {
	multiplier int64
}

func (s Multiplication) FactoryID() int32 {
	return multiplicationFactoryID
}

func (s Multiplication) ClassID() int32 {
	return multiplicationProcessorClassID
}

func (s Multiplication) WriteData(output serialization.DataOutput) {
	output.WriteInt64(s.multiplier)
}

func (s *Multiplication) ReadData(input serialization.DataInput) {
	s.multiplier = input.ReadInt64()
}

type MultiplicationFactory struct {
}

func (f MultiplicationFactory) Create(id int32) serialization.IdentifiedDataSerializable {
	if id == multiplicationProcessorClassID {
		return &Multiplication{}
	}
	panic(fmt.Sprintf("unknown class ID: %d", id))
}

func (f MultiplicationFactory) FactoryID() int32 {
	return multiplicationFactoryID
}
