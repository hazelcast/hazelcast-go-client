package check

import (
	"fmt"
	"math"
	"time"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func NonNegativeInt32(n int) (int32, error) {
	if n < 0 {
		return 0, ihzerrors.NewIllegalArgumentError(fmt.Sprintf("non-negative integer number expected: %d", n), nil)
	}
	if n > math.MaxInt32 {
		return 0, ihzerrors.NewIllegalArgumentError(fmt.Sprintf("signed 32-bit integer number expected: %d", n), nil)
	}
	return int32(n), nil
}

func NonNegativeDuration(v *types.Duration, d time.Duration, msg string) error {
	if *v < 0 {
		return fmt.Errorf("%s: %w", msg, hzerrors.ErrIllegalArgument)
	}
	if *v == 0 {
		*v = types.Duration(d)
	}
	return nil
}

func WithinRangeInt32(n, start, end int32) error {
	if n < start || n > end {
		return ihzerrors.NewIllegalArgumentError(fmt.Sprintf("number %d is out of range [%d,%d]", n, start, end), nil)
	}
	return nil
}
