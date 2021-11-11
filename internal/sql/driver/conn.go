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
	"fmt"

	"github.com/hazelcast/hazelcast-go-client"
)

var (
	_ driver.Conn = (*Conn)(nil)
)

type Conn struct {
	client *hazelcast.Client
}

func newConn(name string) (*Conn, error) {
	config := hazelcast.Config{}
	client, err := hazelcast.StartNewClientWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("starting Hazelcast client: %w", err)
	}
	return &Conn{client: client}, nil
}

func (c Conn) Prepare(query string) (driver.Stmt, error) {
	return newStatement(query, c.client), nil
}

func (c *Conn) Close() error {
	return c.client.Shutdown(context.Background())
}

func (c Conn) Begin() (driver.Tx, error) {
	// we don't support transactions
	return nil, driver.ErrSkip
}
