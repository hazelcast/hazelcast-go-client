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

package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPortRange_Parse(t *testing.T) {
	testCases := []struct {
		E error
		S string
		T PortRange
	}{
		{S: "", T: PortRange{Begin: 5701, End: 5703}},
		{S: "5707", T: PortRange{Begin: 5707, End: 5707}},
		{S: "5702-", T: PortRange{Begin: 5702, End: 5703}},
		{S: "-5705", T: PortRange{Begin: 5701, End: 5705}},
		// note that we don't handle the case when begin > end
		{S: "5707-", T: PortRange{Begin: 5707, End: 5703}},
		// note that we don't handle the case when begin > end
		{S: "-5700", T: PortRange{Begin: 5701, End: 5700}},
		{S: "X", T: PortRange{}, E: errors.New("")},
		{S: "5701-X", T: PortRange{}, E: errors.New("")},
		{S: "X-5701", T: PortRange{}, E: errors.New("")},
	}
	for _, tc := range testCases {
		t.Run(tc.S, func(t *testing.T) {
			pr := PortRange{}
			err := pr.Parse(tc.S)
			if tc.E != nil {
				if err == nil {
					t.Fatalf("should have failed")
				}
				return
			} else if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tc.T, pr)
		})
	}
}
