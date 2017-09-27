package core

import (
	"github.com/hazelcast/go-client/internal"
)

type IDistributedObject interface {
	Constructor(client *internal.HazelcastClient, name *string) interface{}
	Destroy()
	GetName() string
	GetPartitionKey() string
	GetServiceName() string
}
