// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

const ClientType = "GOO"

func DataCalculateSize(d *Data) int {
	return len(d.Buffer()) + Int32SizeInBytes
}

func StringCalculateSize(str *string) int {
	return len(*str) + Int32SizeInBytes
}

func Int64CalculateSize(v int64) int {
	return Int64SizeInBytes
}

func AddressCalculateSize(a *Address) int {
	dataSize := 0
	dataSize += StringCalculateSize(&a.host)
	dataSize += Int32SizeInBytes
	return dataSize
}
