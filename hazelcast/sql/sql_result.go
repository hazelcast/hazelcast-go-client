package sql

import "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"

type Result interface {
	Rows() hztypes.Iterator
}
