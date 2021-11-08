package hazelcast

import "context"

func flagsSetOrClear(flags *int32, flag int32, enable bool) {
	if enable {
		*flags |= flag
	} else {
		*flags &^= flag
	}
}

// extractLockID extracts lock ID from the context.
// If the lock ID is not found, it returns the default lock ID.
func extractLockID(ctx context.Context) int64 {
	if ctx == nil {
		return defaultLockID
	}
	if lockIDValue := ctx.Value(lockIDKey); lockIDValue == nil {
		return defaultLockID
	} else if lid, ok := lockIDValue.(lockID); !ok {
		return defaultLockID
	} else {
		return int64(lid)
	}
}
