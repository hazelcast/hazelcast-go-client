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

package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/client"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

const (
	driverName = "hazelcast"
)

var (
	_                driver.Driver = (*Driver)(nil)
	cursorBufferSize int32         = 4096
	timeoutMillis    int64         = -1
)

func CursorBufferSize() int32 {
	return atomic.LoadInt32(&cursorBufferSize)
}

func SetCursorBufferSize(size int32) {
	atomic.StoreInt32(&cursorBufferSize, size)
}

func TimeoutMillis() int64 {
	return atomic.LoadInt64(&timeoutMillis)
}

func SetTimeoutMillis(ms int64) {
	atomic.StoreInt64(&timeoutMillis, ms)
}

type Driver struct {
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	return newConn(name)
}

func ParseDSN(dsn string) (*client.Config, error) {
	// TODO: remove hazelcast dependency
	config := hazelcast.Config{}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	segs := strings.Split(dsn, ";")
	if len(segs) > 0 {
		// if there are more than 1 segment, then the first one must be addresses
		if segs[0] != "" {
			addrs := strings.Split(segs[0], ",")
			config.Cluster.Network.SetAddresses(addrs...)
		}
		for _, seg := range segs[1:] {
			kv := strings.SplitN(seg, "=", 2)
			if len(kv) != 2 {
				return nil, ihzerrors.NewIllegalArgumentError(fmt.Sprintf("invalid option: %s", seg), nil)
			}
			k, v := kv[0], kv[1]

			switch k {
			case "ClusterName":
				config.Cluster.Name = v
			case "Cluster.Unisocket":
				b, err := strconv.ParseBool(v)
				if err != nil {
					return nil, ihzerrors.NewIllegalArgumentError("invalid Cluster.Unisocket", err)
				}
				config.Cluster.Unisocket = b
			}
		}
	}
	return &client.Config{
		Name:          config.ClientName,
		Cluster:       &config.Cluster,
		Failover:      &config.Failover,
		Serialization: &config.Serialization,
		Logger:        &config.Logger,
		Labels:        config.Labels,
		StatsEnabled:  config.Stats.Enabled,
		StatsPeriod:   time.Duration(config.Stats.Period),
	}, nil
}

func init() {
	sql.Register(driverName, &Driver{})
}
