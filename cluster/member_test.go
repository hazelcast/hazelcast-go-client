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

package cluster_test

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/types"
	"github.com/stretchr/testify/assert"
)

func TestMemberInfo_PublicAddress(t *testing.T) {
	miWithPublicAddr := cluster.MemberInfo{
		AddressMap: map[cluster.EndpointQualifier]cluster.Address{
			cluster.EndpointQualifier{
				Identifier: "public",
				Type:       cluster.EndpointQualifierTypeClient,
			}: "33.32.31.21",
		},
		Address: "100.15.20.33",
		UUID:    types.NewUUIDWith(343243432, 3434333),
		Version: cluster.MemberVersion{Major: 1, Minor: 1},
	}
	miWithoutPublicAddr := cluster.MemberInfo{
		AddressMap: map[cluster.EndpointQualifier]cluster.Address{
			cluster.EndpointQualifier{
				Identifier: "other",
				Type:       cluster.EndpointQualifierTypeClient,
			}: "33.32.31.21",
		},
		Address: "100.15.20.33",
		UUID:    types.NewUUIDWith(343243432, 3434333),
		Version: cluster.MemberVersion{Major: 1, Minor: 1},
	}
	testCases := []struct {
		L  string
		A  cluster.Address
		M  cluster.MemberInfo
		Ok bool
	}{
		{L: "Default member info", M: cluster.MemberInfo{}, Ok: false, A: ""},
		{L: "Member info with public addr", M: miWithPublicAddr, Ok: true, A: cluster.Address("33.32.31.21")},
		{L: "Member info without public addr", M: miWithoutPublicAddr, Ok: false, A: ""},
	}
	for _, tc := range testCases {
		t.Run(tc.L, func(t *testing.T) {
			addr, ok := tc.M.PublicAddress()
			assert.Equal(t, tc.Ok, ok)
			assert.Equal(t, tc.A, addr)
		})
	}
}
