/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package internal

import "time"

// TimeMillis converts the time to milliseconds.
// This function is required by Go < 1.17.
// Go 1.17 has t.UnixMilli(), so use that once min Go supported is 1.17.
func TimeMillis(t time.Time) int64 {
	return t.Unix()*1e3 + int64(t.Nanosecond())/1e6
}
