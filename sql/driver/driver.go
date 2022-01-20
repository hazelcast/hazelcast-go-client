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

// Open creates the driver with the given client configuration.
// This function is provided as a convenience to set configuration items which cannot be set using a DSN.
// Note that, attached listeners are ignored.
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

/*
WithCursorBufferSize returns a copy of parent context which includes the given query cursor buffer size.
When rows are ready to be consumed, they are put into an internal buffer of the cursor.
This parameter defines the maximum number of rows in that buffer.
When the threshold is reached, the backpressure mechanism will slow down the execution, possibly to a complete halt, to prevent out-of-memory.
The default value is expected to work well for most workloads.
A bigger buffer size may give you a slight performance boost for queries with large result sets at the cost of increased memory consumption.
Defaults to 4096.
Panics if parent context is nil, or given buffer size is not in the positive int32 range.
*/
func WithCursorBufferSize(parent context.Context, cbs int) context.Context {
	if parent == nil {
		panic(ihzerrors.NewIllegalArgumentError("parent context is nil", nil))
	}
	v, err := check.NonNegativeInt32(cbs)
	if err != nil {
		panic(err)
	}
	return context.WithValue(parent, driver.QueryCursorBufferSizeKey{}, v)
}

/*
WithQueryTimeout returns a copy of parent context which has the given query execution timeout.
If the timeout is reached for a running statement, it will be cancelled forcefully.
Zero value means no timeout.
Negative values mean that the value from the server-side config will be used.
Defaults to -1
Panics if parent context is nil.
*/
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

/*
WithSchema returns a copy of parent context which has the given schema name.
The engine will try to resolve the non-qualified object identifiers from the statement in the given schema.
If not found, the default search path will be used.
The schema name is case-sensitive. For example, foo and Foo are different schemas.
By default, only the default search path is used.
*/
func WithSchema(parent context.Context, schema string) context.Context {
	if parent == nil {
		panic(ihzerrors.NewIllegalArgumentError("parent context is nil", nil))
	}
	return context.WithValue(parent, driver.QuerySchemaKey{}, schema)
}

// SetSerializationConfig stores the global serialization config.
// Subsequent sql.Open calls will use the given serialization configuration.
// It copies the configuration before storing.
func SetSerializationConfig(config *serialization.Config) error {
	return driver.SetSerializationConfig(config)
}

// SetLoggerConfig stores the logger config.
// Subsequent sql.Open calls will use the given logger configuration.
// It copies the configuration before storing.
func SetLoggerConfig(config *logger.Config) error {
	return driver.SetLoggerConfig(config)
}

// SetSSLConfig stores the SSL config.
// Subsequent sql.Open calls will use the given SSL configuration.
// It copies the configuration before storing.
func SetSSLConfig(config *cluster.SSLConfig) error {
	return driver.SetSSLConfig(config)
}
