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

func TestReconnectMode_MarshalText(t *testing.T) {
	testCases := []struct {
		info     string
		expected []byte
		rm       cluster.ReconnectMode
		hasErr   bool
	}{
		{info: "reconnection-mode on", expected: []byte(`on`), rm: cluster.ReconnectModeOn, hasErr: false},
		{info: "reconnection-mode off", expected: []byte(`off`), rm: cluster.ReconnectModeOff, hasErr: false},
		{info: "reconnection-mode invalid", expected: nil, rm: cluster.ReconnectModeOff + 1, hasErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			got, err := tc.rm.MarshalText()
			if err != nil {
				if tc.hasErr {
					return
				}
				t.Fatal(err)
			} else if err == nil && tc.hasErr {
				t.Fatal("got nil want an error")
			}
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestReconnectMode_UnmarshalText(t *testing.T) {
	testCases := []struct {
		info     string
		input    []byte
		expected cluster.ReconnectMode
		hasErr   bool
	}{
		{info: "reconnection-mode on", input: []byte(`on`), expected: cluster.ReconnectModeOn, hasErr: false},
		{info: "reconnection-mode off", input: []byte(`off`), expected: cluster.ReconnectModeOff, hasErr: false},
		{info: "reconnection-mode invalid", input: []byte(`invalid`), expected: -1, hasErr: true},
	}
	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			rm := cluster.ReconnectMode(0)
			err := rm.UnmarshalText(tc.input)
			if err != nil {
				if tc.hasErr {
					return
				}
				t.Fatal(err)
			} else if err == nil && tc.hasErr {
				t.Fatal("got nil want to error")
			}
			assert.Equal(t, tc.expected, rm)
		})
	}
}
