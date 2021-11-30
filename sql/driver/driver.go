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

package driver

import (
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/sql/driver"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

// CursorBufferSize returns the current cursor buffer size for the SQL driver.
func CursorBufferSize() int32 {
	return driver.CursorBufferSize()
}

// SetCursorBufferSize sets the cursor buffer size for the SQL driver.
func SetCursorBufferSize(size int32) {
	driver.SetCursorBufferSize(size)
}

// Timeout returns the current query timeout for the SQL driver.
func Timeout() time.Duration {
	return time.Duration(driver.TimeoutMillis() * 1_000_000)
}

// SetTimeout sets the query timeout for the SQL driver.
func SetTimeout(timeout time.Duration) {
	driver.SetTimeoutMillis(timeout.Milliseconds())
}

func SetSerializationConfig(config *serialization.Config) error {
	return driver.SetSerializationConfig(config)
}
