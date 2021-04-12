package invocation

import (
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

type Result interface {
	Get() (*proto.ClientMessage, error)
	GetWithTimeout(duration time.Duration) (*proto.ClientMessage, error)
}
