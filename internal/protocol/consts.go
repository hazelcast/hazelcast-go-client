// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protocol

import (
	. "github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const CLIENT_TYPE = "GO"

func DataCalculateSize(d *Data) int {
	return len(d.Buffer()) + INT_SIZE_IN_BYTES
}
func StringCalculateSize(str *string) int {
	return len(*str) + INT_SIZE_IN_BYTES
}
func AddressCalculateSize(a *Address) int {
	dataSize := 0
	dataSize += StringCalculateSize(&a.host)
	dataSize += INT_SIZE_IN_BYTES
	return dataSize
}
