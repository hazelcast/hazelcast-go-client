package invocation

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"time"
)

type Result interface {
	Get() (*proto.ClientMessage, error)
	GetWithTimeout(duration time.Duration) (*proto.ClientMessage, error)
}
