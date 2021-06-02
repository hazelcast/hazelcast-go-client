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
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
)

func TestAddressParse(t *testing.T) {
	testCases := []struct {
		err   error
		input string
		host  string
		port  int
	}{
		{input: "", host: "", port: 0, err: errors.New("parsing address: missing port in address")},
		{input: "localhost", host: "", port: 0, err: errors.New("parsing address: address localhost: missing port in address")},
		{input: "localhost:5701", host: "localhost", port: 5701},
		{input: "foo.com:2223", host: "foo.com", port: 2223},
		{input: ":4566", host: "", port: 4566},
		// TODO: ipv6
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			addr, err := cluster.ParseAddress(tc.input)
			if tc.err != nil {
				if err == nil {
					t.Fatalf("should have failed")
				}
				if tc.err.Error() != err.Error() {
					t.Fatalf("target err: %v != %v", tc.err, err)
				}
				return
			} else if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, tc.host, addr.Host())
			assert.Equal(t, tc.port, addr.Port())
		})
	}
}
