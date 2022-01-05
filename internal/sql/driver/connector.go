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
	"context"
	"database/sql/driver"
	"sync"

	"github.com/hazelcast/hazelcast-go-client/internal/client"
)

type Connector struct {
	config *client.Config
	conn   *Conn
	drv    *Driver
	mu     *sync.Mutex
}

func NewConnector(config *client.Config) *Connector {
	return &Connector{
		config: config,
		drv:    &Driver{},
		mu:     &sync.Mutex{},
	}
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		conn, err := NewConnWithConfig(ctx, c.config)
		if err != nil {
			return nil, err
		}
		c.conn = conn
	}
	return c.conn, nil
}

func (c *Connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.ic.Shutdown(context.Background())
	}
	return nil
}

func (c Connector) Driver() driver.Driver {
	return c.drv
}
