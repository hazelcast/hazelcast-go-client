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

package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/cluster"
)

func TestMembershipState_String(t *testing.T) {
	for _, s := range []struct {
		info  string
		want  string
		state cluster.MembershipState
	}{
		{info: "membership added state", want: "added", state: cluster.MembershipStateAdded},
		{info: "membership removed state", want: "removed", state: cluster.MembershipStateRemoved},
		{info: "unknown membership state", want: "UNKNOWN", state: 2},
	} {
		t.Run(s.info, func(t *testing.T) {
			assert.Equal(t, s.want, s.state.String())
		})
	}
}

func TestMembershipStateChanged_EventName(t *testing.T) {
	msc := cluster.MembershipStateChanged{
		State: cluster.MembershipStateAdded,
	}
	got := msc.EventName()
	want := "cluster.membershipstatechanged"
	assert.Equal(t, want, got)
}
