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

package nearcache

import "math"

const (
	timeUnset = -1
)

var EpochTimeMillis = zeroOutMs(1514764800000)

func StripBaseTime(ms int64) int32 {
	if ms == math.MaxInt64 {
		return math.MaxInt32
	}
	if ms <= 0 {
		return timeUnset
	}
	toSeconds := (ms - EpochTimeMillis) / 1000
	if toSeconds >= math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(toSeconds)
}

func RecomputeWithBaseTime(seconds int32) int64 {
	if int64(seconds) == timeUnset {
		return 0
	}
	if seconds == math.MaxInt32 {
		return math.MaxInt64
	}
	return EpochTimeMillis + int64(seconds)*1000
}

func zeroOutMs(ms int64) int64 {
	return (ms / 1000) * 1000
}
