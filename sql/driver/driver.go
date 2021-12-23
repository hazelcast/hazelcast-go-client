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
	"database/sql"
	"math"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/check"
	"github.com/hazelcast/hazelcast-go-client/internal/client"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/sql/driver"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func Open(config hazelcast.Config) *sql.DB {
	config = config.Clone()
	// ignoring the error below, since the default configuration does not contain errors
	_ = config.Validate()
	icc := &client.Config{
		Name:          config.ClientName,
		Cluster:       &config.Cluster,
		Failover:      &config.Failover,
		Serialization: &config.Serialization,
		Logger:        &config.Logger,
		Labels:        config.Labels,
		StatsEnabled:  config.Stats.Enabled,
		StatsPeriod:   time.Duration(config.Stats.Period),
	}
	return sql.OpenDB(driver.NewConnector(icc))
}

// WithCursorBufferSize returns a copy of parent in which has the given query cursor buffer size.
// Panics if parent context is nil, or given buffer size is not in the positive int32 range.
func WithCursorBufferSize(parent context.Context, cbs int) context.Context {
	if parent == nil {
		panic(ihzerrors.NewIllegalArgumentError("parent context is nil", nil))
	}
	if err := check.WithinRangeInt32(int32(cbs), 1, math.MaxInt32); err != nil {
		panic(err)
	}
	return context.WithValue(parent, driver.QueryCursorBufferSizeKey{}, int32(cbs))
}

// WithQueryTimeout returns a copy of parent in which has the given query timeout.
// Panics if parent context is nil.
func WithQueryTimeout(parent context.Context, t time.Duration) context.Context {
	if parent == nil {
		panic(ihzerrors.NewIllegalArgumentError("parent context is nil", nil))
	}
	tm := t.Milliseconds()
	if t < 0 {
		tm = -1
	}
	return context.WithValue(parent, driver.QueryTimeoutKey{}, tm)
}

// SetSerializationConfig stores the serialization config.
// It copies the configuration before storing.
func SetSerializationConfig(config *serialization.Config) error {
	return driver.SetSerializationConfig(config)
}

// SetLoggerConfig stores the logger config.
// It copies the configuration before storing.
func SetLoggerConfig(config *logger.Config) error {
	return driver.SetLoggerConfig(config)
}

// SetSSLConfig stores the SSL config.
// It copies the configuration before storing.
func SetSSLConfig(config *cluster.SSLConfig) error {
	return driver.SetSSLConfig(config)
}
