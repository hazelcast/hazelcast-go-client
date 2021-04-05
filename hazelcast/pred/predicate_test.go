package pred_test

import (
	"fmt"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/pred"
)

func TestAnd(t *testing.T) {
	p := pred.And(
		pred.Equal("foo", "Bar"),
		pred.And(
			pred.Equal("va", 10),
			pred.Equal("vb", 15),
		),
		pred.Equal("v1", 5),
	)
	fmt.Println(p)
}
