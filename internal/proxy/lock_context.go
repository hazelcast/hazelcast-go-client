/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package proxy

import "context"

type lockID int64
type lockIDKey struct{}

const (
	defaultLockID = 0
)

var (
	lockIDGen = NewReferenceIDGenerator(1)
)

func NewLockContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, lockIDKey{}, lockID(lockIDGen.NextID()))
}

// ExtractLockID extracts lock ID from the context.
// If the lock ID is not found, it returns the default lock ID.
func ExtractLockID(ctx context.Context) int64 {
	if ctx == nil {
		return defaultLockID
	}
	lidv := ctx.Value(lockIDKey{})
	if lidv == nil {
		return defaultLockID
	}
	lid, ok := lidv.(lockID)
	if !ok {
		return defaultLockID
	}
	return int64(lid)
}
