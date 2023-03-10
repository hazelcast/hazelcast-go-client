/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

	"github.com/hazelcast/hazelcast-go-client/internal/check"
	"github.com/hazelcast/hazelcast-go-client/internal/client"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

var (
	_ driver.Conn = (*Conn)(nil)
)

type Conn struct {
	ic *client.Client
	ss *SQLService
}

func (c *Conn) CheckNamedValue(v *driver.NamedValue) error {
	if check.Nil(v.Value) {
		return ihzerrors.NewIllegalArgumentError("nil arg is not allowed", nil)
	}
	return nil
}

func newConn(name string) (*Conn, error) {
	config, err := MakeConfigFromDSN(name)
	if err != nil {
		return nil, fmt.Errorf("configuring internal client: %w", err)
	}
	return NewConnWithConfig(context.Background(), config)
}

func NewConnWithConfig(ctx context.Context, config *client.Config) (*Conn, error) {
	// TODO: channel size
	schemaCh := make(chan serialization.SchemaMsg)
	ic, err := client.New(config, schemaCh)
	if err != nil {
		return nil, fmt.Errorf("starting Hazelcast client: %w", err)
	}
	if err := ic.Start(ctx); err != nil {
		return nil, err
	}
	return NewConnWithClient(ic), nil
}

func NewConnWithClient(ic *client.Client) *Conn {
	ss := NewSQLService(ic.ConnectionManager, ic.SerializationService, ic.Invoker, &ic.Logger)
	return &Conn{
		ic: ic,
		ss: ss,
	}
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return newStatement(query, c.ss), nil
}

func (c *Conn) Close() error {
	return nil
}

func (c Conn) Begin() (driver.Tx, error) {
	// we don't support transactions
	return nil, driver.ErrSkip
}
