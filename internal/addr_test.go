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
	"errors"
	"fmt"
	"net"
	"strconv"
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
		{Addr: "foo:", Target: parseAddrTarget{Host: "", Port: 0, Err: &strconv.NumError{Func: "Atoi", Num: "", Err: errors.New("invalid syntax")}}},
		{Addr: ":1234", Target: parseAddrTarget{Host: "127.0.0.1", Port: 1234, Err: nil}},
		{Addr: "", Target: parseAddrTarget{Host: "127.0.0.1", Port: 0, Err: nil}},
		{Addr: "169.254.172.39", Target: parseAddrTarget{Host: "169.254.172.39", Port: 0, Err: nil}},
		{Addr: "169.254.172.39:5703", Target: parseAddrTarget{Host: "169.254.172.39", Port: 5703, Err: nil}},
		{Addr: "169.254.172.39:", Target: parseAddrTarget{Host: "", Port: 0, Err: &strconv.NumError{Func: "Atoi", Num: "", Err: errors.New("invalid syntax")}}},
		{Addr: "[fe80::43:ecff:fec9:1683]:5705", Target: parseAddrTarget{Host: "fe80::43:ecff:fec9:1683", Port: 5705, Err: nil}},
		{Addr: "[fe80::43:ecff:fec9:1683]:", Target: parseAddrTarget{Host: "", Port: 0, Err: &strconv.NumError{Func: "Atoi", Num: "", Err: errors.New("invalid syntax")}}},
		{Addr: "fe80::43:ecff:fec9:1683", Target: parseAddrTarget{Host: "", Port: 0, Err: &net.AddrError{Err: "too many colons in address", Addr: "fe80::43:ecff:fec9:1683"}}},
		{Addr: "[fe80::43:ecff:fec9:1683", Target: parseAddrTarget{Host: "", Port: 0, Err: &net.AddrError{Err: "missing ']' in address", Addr: "[fe80::43:ecff:fec9:1683"}}},
		{Addr: "fe80::43:ecff:fec9:1683]:5678", Target: parseAddrTarget{Host: "", Port: 0, Err: &net.AddrError{Err: "too many colons in address", Addr: "fe80::43:ecff:fec9:1683]:5678"}}},
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
