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
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/client"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type QueryCursorBufferSizeKey struct{}
type QueryTimeoutKey struct{}

const (
	driverName                    = "hazelcast"
	protocolHz                    = "hz"
	protocolHzViaTLS              = "hz+tls"
	DefaultCursorBufferSize int32 = 4096
	DefaultTimeoutMillis    int64 = -1
	optClusterName                = "cluster.name"
	optUnisocket                  = "unisocket"
	optCloudToken                 = "cloud.token"
	optLog                        = "log"
	optStatsPeriod                = "stats.period"
	optSSL                        = "ssl"
	optSSLCAPath                  = "ssl.ca.path"
	optSSLCertPath                = "ssl.cert.path"
	optSSLKeyPath                 = "ssl.key.path"
	optSSLKeyPassword             = "ssl.key.password"
)

var (
	_                   driver.Driver = (*Driver)(nil)
	serializationConfig atomic.Value
	loggerConfig        atomic.Value
	sslConfig           atomic.Value
)

// SerializationConfig returns the current serialization config.
// Note that it doesn't return a copy.
func SerializationConfig() *serialization.Config {
	sc := serializationConfig.Load()
	if sc == nil {
		return nil
	}
	return sc.(*serialization.Config)
}

// SetSerializationConfig stores the serialization config.
// It copies the configuration before storing.
func SetSerializationConfig(c *serialization.Config) error {
	if c == nil {
		serializationConfig.Store(c)
		return nil
	}
	cc := c.Clone()
	if err := cc.Validate(); err != nil {
		return err
	}
	serializationConfig.Store(&cc)
	return nil
}

// LoggerConfig returns the current logger config.
// Note that it doesn't return a copy.
func LoggerConfig() *logger.Config {
	lc := loggerConfig.Load()
	if lc == nil {
		return nil
	}
	return lc.(*logger.Config)
}

// SetLoggerConfig stores the logger config.
// It copies the configuration before storing.
func SetLoggerConfig(c *logger.Config) error {
	if c == nil {
		loggerConfig.Store(c)
		return nil
	}
	cc := c.Clone()
	if err := cc.Validate(); err != nil {
		return err
	}
	loggerConfig.Store(&cc)
	return nil
}

// SSLConfig returns the current SSL config.
// Note that it doesn't return a copy.
func SSLConfig() *cluster.SSLConfig {
	sc := sslConfig.Load()
	if sc == nil {
		return nil
	}
	return sc.(*cluster.SSLConfig)
}

// SetSSLConfig stores the SSL config.
// It copies the configuration before storing.
func SetSSLConfig(c *cluster.SSLConfig) error {
	if c == nil {
		sslConfig.Store(c)
		return nil
	}
	cc := c.Clone()
	if err := cc.Validate(); err != nil {
		return err
	}
	sslConfig.Store(&cc)
	return nil
}

type Driver struct{}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	config, err := MakeConfigFromDSN(name)
	if err != nil {
		return nil, err
	}
	return NewConnector(config), nil
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	return newConn(name)
}

func MakeConfigFromDSN(dsn string) (*client.Config, error) {
	config := client.NewConfig()
	if lc := LoggerConfig(); lc != nil {
		lcc := lc.Clone()
		config.Logger = &lcc
	}
	if sc := SSLConfig(); sc != nil {
		config.Cluster.Network.SSL = sc.Clone()
	}
	if dsn != "" {
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, fmt.Errorf("parsing DSN: %w", err)
		}
		if err := parseDSNScheme(config.Cluster, u.Scheme); err != nil {
			return nil, fmt.Errorf("parsing DSN: %w", err)
		}
		if u.Host != "" {
			config.Cluster.Network.SetAddresses(strings.Split(u.Host, ",")...)
		}
		// path part of the DSN should be blank
		if u.Path != "" {
			return nil, errors.New("parsing DSN: path is not allowed")
		}
		updateCredentials(config.Cluster, u.User)
		if err := updateConfig(config, u.Query()); err != nil {
			return nil, fmt.Errorf("parsing DSN options: %w", err)
		}
	}
	sc := SerializationConfig()
	if sc == nil {
		sc = config.Serialization
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return config, nil
}

func parseDSNScheme(config *cluster.Config, scheme string) error {
	switch scheme {
	case "":
		return fmt.Errorf("scheme is required")
	case protocolHz:
	case protocolHzViaTLS:
		config.Network.SSL.Enabled = true
	default:
		return fmt.Errorf("unknown scheme: %s", scheme)
	}
	return nil
}

func updateCredentials(config *cluster.Config, ui *url.Userinfo) {
	// ignoring the second return value, since pass is blank if it's true or false.
	pass, _ := ui.Password()
	creds := &config.Security.Credentials
	creds.Username = ui.Username()
	creds.Password = pass
}

func updateConfig(config *client.Config, values map[string][]string) error {
	for k, vs := range values {
		v := firstString(vs)
		switch k {
		case optClusterName:
			config.Cluster.Name = v
		case optUnisocket:
			b, err := strconv.ParseBool(v)
			if err != nil {
				return ihzerrors.NewIllegalArgumentError(fmt.Sprintf("invalid %s option value", optUnisocket), err)
			}
			config.Cluster.Unisocket = b
		case optCloudToken:
			config.Cluster.Cloud.Enabled = true
			config.Cluster.Cloud.Token = v
		case optLog:
			config.Logger.Level = logger.Level(v)
		case optStatsPeriod:
			config.StatsEnabled = true
			dur, err := time.ParseDuration(v)
			if err != nil {
				return ihzerrors.NewIllegalArgumentError(fmt.Sprintf("invalid %s option value", optStatsPeriod), err)
			}
			config.StatsPeriod = dur
		}
	}
	return updateSSLConfig(config, values)
}

func updateSSLConfig(config *client.Config, values map[string][]string) error {
	var caPath, certPath, keyPath, keyPass string
	sslConfig := &config.Cluster.Network.SSL
	if vs, ok := values[optSSL]; ok {
		b, err := strconv.ParseBool(firstString(vs))
		if err != nil {
			return ihzerrors.NewIllegalArgumentError(fmt.Sprintf("invalid %s option value", optSSL), err)
		}
		sslConfig.Enabled = b
	}
	if vs, ok := values[optSSLCAPath]; ok {
		caPath = firstString(vs)
	}
	if vs, ok := values[optSSLCertPath]; ok {
		certPath = firstString(vs)
	}
	if vs, ok := values[optSSLKeyPath]; ok {
		keyPath = firstString(vs)
	}
	if vs, ok := values[optSSLKeyPassword]; ok {
		keyPass = firstString(vs)
	}
	if caPath != "" || certPath != "" || keyPath != "" {
		config.Cluster.Network.SSL.Enabled = true
		if caPath == "" {
			return ihzerrors.NewIllegalArgumentError(fmt.Sprintf("invalid %s value", optSSLCAPath), nil)
		}
		if certPath == "" {
			return ihzerrors.NewIllegalArgumentError(fmt.Sprintf("invalid %s value", optSSLCertPath), nil)
		}
		if keyPath == "" {
			return ihzerrors.NewIllegalArgumentError(fmt.Sprintf("invalid %s value", optSSLKeyPath), nil)
		}
		sslConfig.SetTLSConfig(&tls.Config{ServerName: "hazelcast.cloud"})
		if err := sslConfig.SetCAPath(caPath); err != nil {
			return ihzerrors.NewIllegalArgumentError("setting CA path", err)
		}
		if keyPass != "" {
			if err := sslConfig.AddClientCertAndEncryptedKeyPath(certPath, keyPath, keyPass); err != nil {
				return ihzerrors.NewIllegalArgumentError("setting certificate and encrypted key path", err)
			}
		} else if err := sslConfig.AddClientCertAndKeyPath(certPath, keyPath); err != nil {
			return ihzerrors.NewIllegalArgumentError("setting certificate and key path", err)
		}
	}
	return nil
}

func firstString(ss []string) string {
	if len(ss) == 0 {
		return ""
	}
	return ss[0]
}

func init() {
	sql.Register(driverName, &Driver{})
}
