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
)

const defaultHost = "127.0.0.1"

func ParseAddr(addr string) (string, int, error) {
	if addr == "" || strings.TrimSpace(addr) == "" {
		return defaultHost, 0, nil
	}
	if !hasPort(addr) {
		return addr, 0, nil
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return host, 0, err
	}
	if len(port) == 0 {
		return host, 0, nil
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		return "", 0, err
	}
	if host == "" || strings.TrimSpace(host) == "" {
		host = defaultHost
	}
	if portInt < 0 {
		// the port number must be positive value
		return "", 0, fmt.Errorf("invalid port number: '%d'", portInt)
	}
	return host, portInt, nil
}

func hasPort(addr string) bool {
	i := strings.LastIndex(addr, ":")
	if i == -1 {
		return false
	}
	if i == len(addr)-1 {
		return true
	}
	_, err := strconv.Atoi(addr[i+1:])
	return err == nil
}
