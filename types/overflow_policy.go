/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package types

// OverflowPolicy
// Using this OverflowPolicy one can control the behavior what should to be done
// when an item is about to be added to the RingBuffer, but there is {@code 0}
// remaining capacity.
//
// Overflowing happens when a time-to-live is set and the oldest item in
// the RingBuffer (the head) is not old enough to expire.
type OverflowPolicy int32

// OverflowPolicyOverwrite
// Using this OverflowPolicyOverwrite policy the oldest item is overwritten
// no matter it is not old enough to retire. Using this policy you are
// sacrificing the time-to-live in favor of being able to write.
//
// Example: if there is a time-to-live of 30 seconds, the buffer is full
// and the oldest item in the ring has been placed a second ago, then there
// are 29 seconds remaining for that item. Using this policy you are going
// to overwrite no matter what.
const OverflowPolicyOverwrite OverflowPolicy = 0

// OverflowPolicyFail
// Using this policy the call will fail immediately and the oldest item will
// not be overwritten before it is old enough to retire. So this policy
// sacrificing the ability to write in favor of time-to-live.
//
// The advantage of OverflowPolicyFail is that the caller can decide what to do
// since it doesn't trap the thread due to backoff.
//
// Example: if there is a time-to-live of 30 seconds, the buffer is full
// and the oldest item in the ring has been placed a second ago, then there
// are 29 seconds remaining for that item. Using this policy you are not
// going to overwrite that item for the next 29 seconds.
const OverflowPolicyFail OverflowPolicy = 1
