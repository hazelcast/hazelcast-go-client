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

package internal

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

const defaultHost = "127.0.0.1"

func ParseAddr(addr string) (string, int, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return defaultHost, 0, nil
	}
	host, port, err := splitHostPort(addr)
	if err != nil {
		return "", 0, ihzerrors.NewClientError("", err, hzerrors.ErrInvalidAddress)
	}
	if port < 0 || port > 65535 {
		return "", 0, ihzerrors.NewClientError("port not in valid range", err, hzerrors.ErrInvalidAddress)
	}
	return host, port, nil
}

func splitHostPort(addr string) (host string, port int, err error) {
	idx := strings.LastIndex(addr, ":")
	if idx == -1 {
		return addr, 0, nil
	}
	port, err = strconv.Atoi(addr[idx+1:])
	if err != nil {
		return "", 0, fmt.Errorf("invalid port")
	}
	host, _, err = net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	if host == "" {
		host = defaultHost
	}
	return host, port, nil
}
