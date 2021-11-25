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

	"github.com/hazelcast/hazelcast-go-client/internal/client"
)

var (
	_ driver.Conn = (*Conn)(nil)
)

type Conn struct {
	ic *client.Client
	ss *SQLService
}

func newConn(name string) (*Conn, error) {
	config, err := ParseDSN(name)
	if err != nil {
		return nil, fmt.Errorf("configuring internal client: %w", err)
	}
	ic, err := client.New(config)
	if err != nil {
		return nil, fmt.Errorf("starting Hazelcast client: %w", err)
	}
	if err := ic.Start(context.Background()); err != nil {
		return nil, err
	}
	ss := newSQLService(ic.ConnectionManager, ic.SerializationService, ic.InvocationFactory, ic.InvocationService)
	return &Conn{
		ic: ic,
		ss: ss,
	}, nil
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return newStatement(query, c.ss), nil
}

func (c *Conn) Close() error {
	return c.ic.Shutdown(context.Background())
}

func (c Conn) Begin() (driver.Tx, error) {
	// we don't support transactions
	return nil, driver.ErrSkip
}
