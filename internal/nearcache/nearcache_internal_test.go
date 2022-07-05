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

package nearcache

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestFilterDataMembers(t *testing.T) {
	var uuids []types.UUID
	for i := 0; i < 10; i++ {
		uuids = append(uuids, types.NewUUID())
	}
	testCases := []struct {
		name   string
		mems   []pubcluster.MemberInfo
		target []pubcluster.MemberInfo
	}{
		{
			name:   "empty",
			mems:   []pubcluster.MemberInfo{},
			target: []pubcluster.MemberInfo{},
		},
		{
			name: "single element data",
			mems: []pubcluster.MemberInfo{
				{LiteMember: false},
			},
			target: []pubcluster.MemberInfo{
				{LiteMember: false},
			},
		},
		{
			name: "single element lite",
			mems: []pubcluster.MemberInfo{
				{LiteMember: true},
			},
			target: []pubcluster.MemberInfo{},
		},
		{
			name: "data lite",
			mems: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: false},
				{UUID: uuids[1], LiteMember: true},
			},
			target: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: false},
			},
		},
		{
			name: "lite data",
			mems: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: true},
				{UUID: uuids[1], LiteMember: false},
			},
			target: []pubcluster.MemberInfo{
				{UUID: uuids[1], LiteMember: false},
			},
		},
		{
			name: "data lite data",
			mems: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: false},
				{UUID: uuids[1], LiteMember: true},
				{UUID: uuids[2], LiteMember: false},
			},
			target: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: false},
				{UUID: uuids[2], LiteMember: false},
			},
		},
		{
			name: "lite data lite",
			mems: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: true},
				{UUID: uuids[1], LiteMember: false},
				{UUID: uuids[2], LiteMember: true},
			},
			target: []pubcluster.MemberInfo{
				{UUID: uuids[1], LiteMember: false},
			},
		},
		{
			name: "lite lite data",
			mems: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: true},
				{UUID: uuids[1], LiteMember: true},
				{UUID: uuids[2], LiteMember: false},
			},
			target: []pubcluster.MemberInfo{
				{UUID: uuids[2], LiteMember: false},
			},
		},
		{
			name: "data data lite",
			mems: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: false},
				{UUID: uuids[1], LiteMember: false},
				{UUID: uuids[2], LiteMember: true},
			},
			target: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: false},
				{UUID: uuids[1], LiteMember: false},
			},
		},
		{
			name: "data data data",
			mems: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: false},
				{UUID: uuids[1], LiteMember: false},
				{UUID: uuids[2], LiteMember: false},
			},
			target: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: false},
				{UUID: uuids[1], LiteMember: false},
				{UUID: uuids[2], LiteMember: false},
			},
		},
		{
			name: "lite lite lite",
			mems: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: true},
				{UUID: uuids[1], LiteMember: true},
				{UUID: uuids[2], LiteMember: true},
			},
			target: []pubcluster.MemberInfo{},
		},
		{
			name: "lite lite lite data lite lite lite",
			mems: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: true},
				{UUID: uuids[1], LiteMember: true},
				{UUID: uuids[2], LiteMember: true},
				{UUID: uuids[3], LiteMember: false},
				{UUID: uuids[4], LiteMember: true},
				{UUID: uuids[5], LiteMember: true},
				{UUID: uuids[6], LiteMember: true},
			},
			target: []pubcluster.MemberInfo{
				{UUID: uuids[3], LiteMember: false},
			},
		},
		{
			name: "data data data lite data data data",
			mems: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: false},
				{UUID: uuids[1], LiteMember: false},
				{UUID: uuids[2], LiteMember: false},
				{UUID: uuids[3], LiteMember: true},
				{UUID: uuids[4], LiteMember: false},
				{UUID: uuids[5], LiteMember: false},
				{UUID: uuids[6], LiteMember: false},
			},
			target: []pubcluster.MemberInfo{
				{UUID: uuids[0], LiteMember: false},
				{UUID: uuids[1], LiteMember: false},
				{UUID: uuids[2], LiteMember: false},
				{UUID: uuids[4], LiteMember: false},
				{UUID: uuids[5], LiteMember: false},
				{UUID: uuids[6], LiteMember: false},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := filterDataMembers(tc.mems)
			// sort by UUID
			sort.SliceStable(r, func(i, j int) bool {
				return r[i].UUID.String() < r[j].UUID.String()
			})
			sort.SliceStable(tc.target, func(i, j int) bool {
				return tc.target[i].UUID.String() < tc.target[j].UUID.String()
			})
			assert.Equal(t, tc.target, r)
		})
	}
}