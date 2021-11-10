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

package internal_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal"
)

type parseAddrTarget struct {
	Err  error
	Host string
	Port int
}

func TestParseAddr(t *testing.T) {
	testCases := []struct {
		Addr   string
		Target parseAddrTarget
	}{
		{Addr: "foo:1234", Target: parseAddrTarget{Host: "foo", Port: 1234, Err: nil}},
		{Addr: "foo", Target: parseAddrTarget{Host: "foo", Port: 0, Err: nil}},
		{Addr: ":1234", Target: parseAddrTarget{Host: "127.0.0.1", Port: 1234, Err: nil}},
		{Addr: "", Target: parseAddrTarget{Host: "127.0.0.1", Port: 0, Err: nil}},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("addr: %s", tc.Addr), func(t *testing.T) {
			host, port, err := internal.ParseAddr(tc.Addr)
			assert.Equal(t, tc.Target.Err, err)
			assert.Equal(t, tc.Target.Host, host)
			assert.Equal(t, tc.Target.Port, port)
		})
	}
}
