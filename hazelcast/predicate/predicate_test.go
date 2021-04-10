package predicate_test

import (
	"fmt"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/predicate"
)

func TestAnd(t *testing.T) {
	p := predicate.And(
		predicate.Equal("foo", "Bar"),
		predicate.And(
			predicate.Equal("va", 10),
			predicate.Equal("vb", 15),
		),
		predicate.Equal("v1", 5),
	)
	fmt.Println(p)
}
