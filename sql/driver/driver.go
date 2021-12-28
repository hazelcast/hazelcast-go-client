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

/*
Package driver provides an SQL driver compatible with the standard database/sql package.

This driver supports Hazelcast 5.0 and up. Checkout the Hazelcast SQL documentation here: https://docs.hazelcast.com/hazelcast/5.0/sql/sql-overview

Enabling Hazelcast SQL

The SQL support should be enabled in Hazelcast configuration:

	<hazelcast>
		<jet enabled="true" />
	</hazelcast>

Creating a Driver Instance Using sql.Open

This driver provides two ways to create an instance.

The first one is via the standard sql.Open function.
That function takes two parameters, the driver name and the DSN (Data Source Name).
Here's a sample:

	db, err := sql.Open("hazelcast", "hz://@localhost:5701?cluster.name=dev")

Use hazelcast as the driver name.
The DSN may be blank. In that case, the default configuration is used.
Otherwise, the DSN must start with the scheme (hz://) and have the following optional parts:

	- Username and password for the cluster, separated by a column: dave:s3cr3t
	- Hazelcast member addresses, separated by commas: server1:port1,server2:port2
	- Options as key=value pairs, separated by ampersond (&). Both the key and value must be URL encoded: cluster.name=dev&ssl=true

Username/password part is separated from the address by the at sign (@).
There should be a question mark (?) between the address(es) and options.
Here is a full DSN:

	hz://dave:s3cr3t@my-server1.company.com:5000,my-server2.company.com:6000?cluster.name=prod&ssl=true&log=warn

The following are the available options:

	- unisocket: A boolean. Enables/disables the unisocket mode. Default: false. Example: unisocket=true
	- log: One of the following: off, fatal, error, warn, info, debug, trace. Default: info. Example: log=debug
	- cluster.name: A string. Specifies the cluster name.Default: dev. Example: cluster.name=hzc1
	- cloud.token: A string. Sets the Hazelcast Cloud token. Example: cloud.token=1234567abcde
	- stats.period: Duration, which can be parsed by time.Parse.
	  Use one of the following suffixes: s (seconds), m (minutes), h (hours). Example: 10s
	- ssl: A boolean. Enables/disables SSL connections. Defaults: false. Example: ssl=true
	- ssl.ca.path: The path to the PEM file for the certificate authority. Implies ssl=true. Example: ssl.ca.path=/etc/ssl/ca.pem
	- ssl.cert.path: The path to the TLS certificate. Implies ssl=true. Example: ssl.cert.path=/etc/ssl/cert.pem
	- ssl.key.path: The path to the certificate key. Implies ssl=true. Example: ssl.key.path=/etc/ssl/key.pem
	- ssl.key.password: The optional certificate password. Example: ssl.key.password=m0us3

Some items in the client configuration cannot be set in the DSN, such as serialization factories and SSL configuration.
You can use the following functions to set those configuration items globally:

	- SetSerializationConfig(...)
	- SetLoggerConfig(...)
	- SetSSLConfig(...)

Note that, these functions changes affect only the subsequent sql.Open calls, not the previous ones.

Here's an example:

	sc1 := &serialization.Config{}
	sc2 := &serialization.Config{}
	// no serialization configuration is used for the call below
	db1, err := sql.Open("hazelcast", "")
	// the following two sql.Open calls use sc1
	err = driver.SetSerializationConfig(sc1)
	db2, err := sql.Open("hazelcast", "")
	db3, err := sql.Open("hazelcast", "")
	// the following sql.Open call uses sc2
	err = driver.SetSerializationConfig(sc2)
	db4, err := sql.Open("hazelcast", "")

Creating a Driver Instance Using driver.Open

It is possible to create a driver instance using an existing Hazelcast client configuration using the driver.Open function.
All client configuration items, except listeners are supported.

	cfg := hazelcast.Config{}
	cfg.Cluster.Name = "prod"
	cfg.Serialization.SetPortableFactories(&MyPortableFactory{})
	db := driver.Open(cfg)



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

// WithCursorBufferSize returns a copy of parent context which includes the given query cursor buffer size.
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

// WithQueryTimeout returns a copy of parent context which has the given query timeout.
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
