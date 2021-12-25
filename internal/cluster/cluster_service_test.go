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

	"github.com/stretchr/testify/assert"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
)

func TestLargerGroupVersion(t *testing.T) {
	testCases := []struct {
		Name             string
		ErrorText        string
		Members          []pubcluster.MemberInfo
		TargetMajorMinor uint16
	}{
		{
			Name:    "no members",
			Members: []pubcluster.MemberInfo{},
		},
		{
			Name: "only lite members",
			Members: []pubcluster.MemberInfo{
				{LiteMember: true},
			},
		},
		{
			Name: "single member",
			Members: []pubcluster.MemberInfo{
				{Version: pubcluster.MemberVersion{Major: 5, Minor: 3}},
			},
			TargetMajorMinor: pubcluster.MemberVersion{Major: 5, Minor: 3}.MajorMinor(),
		},
		{
			Name: "two members, same major.minor, different patch",
			Members: []pubcluster.MemberInfo{
				{Version: pubcluster.MemberVersion{Major: 5, Minor: 3, Patch: 1}},
				{Version: pubcluster.MemberVersion{Major: 5, Minor: 3, Patch: 2}},
			},
			TargetMajorMinor: pubcluster.MemberVersion{Major: 5, Minor: 3}.MajorMinor(),
		},
		{
			Name: "three members, 2 different major.minor - 1",
			Members: []pubcluster.MemberInfo{
				{Version: pubcluster.MemberVersion{Major: 5, Minor: 3, Patch: 1}},
				{Version: pubcluster.MemberVersion{Major: 4, Minor: 2, Patch: 2}},
				{Version: pubcluster.MemberVersion{Major: 4, Minor: 2, Patch: 2}},
			},
			TargetMajorMinor: pubcluster.MemberVersion{Major: 4, Minor: 2}.MajorMinor(),
		},
		{
			Name: "three members, 2 different major.minor - 2",
			Members: []pubcluster.MemberInfo{
				{Version: pubcluster.MemberVersion{Major: 4, Minor: 2, Patch: 2}},
				{Version: pubcluster.MemberVersion{Major: 5, Minor: 3, Patch: 1}},
				{Version: pubcluster.MemberVersion{Major: 4, Minor: 2, Patch: 2}},
			},
			TargetMajorMinor: pubcluster.MemberVersion{Major: 4, Minor: 2}.MajorMinor(),
		},
		{
			Name: "three members, 3 different major.minor",
			Members: []pubcluster.MemberInfo{
				{Version: pubcluster.MemberVersion{Major: 5, Minor: 3, Patch: 1}},
				{Version: pubcluster.MemberVersion{Major: 4, Minor: 2, Patch: 2}},
				{Version: pubcluster.MemberVersion{Major: 4, Minor: 1, Patch: 2}},
			},
			ErrorText: "more than 2 distinct member versions found: 5.3.1, 4.2.2, 4.1.2",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			v, err := cluster.LargerGroupMajorMinorVersion(tc.Members)
			if tc.ErrorText != "" {
				if tc.ErrorText != err.Error() {
					t.Fatalf("err: %s != %s", tc.ErrorText, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tc.TargetMajorMinor, v)
		})
	}
}
