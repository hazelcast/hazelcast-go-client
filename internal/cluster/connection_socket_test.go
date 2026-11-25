//go:build linux || darwin

/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
	"net"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
)

func TestConnection_CreateSocketNoDelay(t *testing.T) {
	testCases := []struct {
		name    string
		noDelay bool
	}{
		{name: "NagleEnabledByDefault", noDelay: false},
		{name: "NoDelay", noDelay: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer ln.Close()
			cfg := &pubcluster.NetworkConfig{NoDelay: tc.noDelay}
			conn := &Connection{}
			socket, err := conn.createSocket(cfg, pubcluster.Address(ln.Addr().String()))
			require.NoError(t, err)
			defer socket.Close()
			raw, err := socket.(*net.TCPConn).SyscallConn()
			require.NoError(t, err)
			var value int
			var optErr error
			require.NoError(t, raw.Control(func(fd uintptr) {
				value, optErr = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_NODELAY)
			}))
			require.NoError(t, optErr)
			require.Equal(t, tc.noDelay, value != 0)
		})
	}
}
