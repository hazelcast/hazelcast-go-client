package cp

import (
	"context"
)

// SubSystem represent the CP service.
type SubSystem interface {
	GetAtomicLong(ctx context.Context, name string) (AtomicLong, error)
}
