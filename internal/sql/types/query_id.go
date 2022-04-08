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

import "github.com/hazelcast/hazelcast-go-client/types"

type QueryID struct {
	MemberIDHigh int64
	MemberIDLow  int64
	LocalIDHigh  int64
	LocalIDLow   int64
}

func NewQueryIDFromUUID(uuid types.UUID) QueryID {
	local := types.NewUUID()
	return QueryID{
		MemberIDHigh: int64(uuid.MostSignificantBits()),
		MemberIDLow:  int64(uuid.LeastSignificantBits()),
		LocalIDHigh:  int64(local.MostSignificantBits()),
		LocalIDLow:   int64(local.LeastSignificantBits()),
	}
}
