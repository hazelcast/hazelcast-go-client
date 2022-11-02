package cp

import "context"

type proxy interface {
	Name() string
	ServiceName() string
	Destroy(ctx context.Context) error
}
