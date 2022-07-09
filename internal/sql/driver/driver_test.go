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

package driver_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/skip"
	idriver "github.com/hazelcast/hazelcast-go-client/internal/sql/driver"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	pubdriver "github.com/hazelcast/hazelcast-go-client/sql/driver"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestArgNotNil(t *testing.T) {
	var b *big.Int
	nv := &driver.NamedValue{
		Name:    "",
		Ordinal: 0,
		Value:   b,
	}
	conn := idriver.Conn{}
	err := conn.CheckNamedValue(nv)
	assert.Error(t, err)
	assert.Equal(t, "nil arg is not allowed: illegal argument error", err.Error())
}

func TestParseDSN(t *testing.T) {
	testCases := []struct {
		Err           error
		Cluster       *cluster.Config
		Logger        *logger.Config
		Serialization *serialization.Config
		SSL           *cluster.SSLConfig
		Pre           func()
		Post          func()
		DSN           string
	}{
		{
			DSN: "",
			Cluster: &cluster.Config{
				Name: "dev",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"127.0.0.1:5701"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
				},
			},
		},
		{
			DSN:           "",
			Serialization: &serialization.Config{PortableVersion: 2},
			Pre: func() {
				if err := pubdriver.SetSerializationConfig(&serialization.Config{PortableVersion: 2}); err != nil {
					panic(err)
				}
			},
			Post: func() {
				if err := pubdriver.SetSerializationConfig(nil); err != nil {
					panic(err)
				}
			},
		},
		{
			DSN:    "",
			Logger: &logger.Config{Level: logger.ErrorLevel},
			Pre: func() {
				if err := pubdriver.SetLoggerConfig(&logger.Config{Level: logger.ErrorLevel}); err != nil {
					panic(err)
				}
			},
			Post: func() {
				if err := pubdriver.SetSerializationConfig(nil); err != nil {
					panic(err)
				}
			},
		},
		{
			DSN: "",
			SSL: &cluster.SSLConfig{Enabled: true},
			Pre: func() {
				if err := pubdriver.SetSSLConfig(&cluster.SSLConfig{Enabled: true}); err != nil {
					panic(err)
				}
			},
			Post: func() {
				if err := pubdriver.SetSSLConfig(nil); err != nil {
					panic(err)
				}
			},
		},
		{
			DSN: "hz://",
			Cluster: &cluster.Config{
				Name: "dev",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"127.0.0.1:5701"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
					SSL:               cluster.SSLConfig{Enabled: false},
				},
			},
		},
		{
			DSN: "hz+tls://",
			Cluster: &cluster.Config{
				Name: "dev",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"127.0.0.1:5701"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
					SSL:               cluster.SSLConfig{Enabled: true},
				},
			},
		},
		{
			DSN: "hz://localhost",
			Cluster: &cluster.Config{
				Name: "dev",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"localhost"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
				},
			},
		},
		{
			DSN: "hz://10.20.30.40:5000",
			Cluster: &cluster.Config{
				Name: "dev",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"10.20.30.40:5000"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
				},
			},
		},
		{
			DSN: "hz://10.20.30.40:5000,11.21.31.41:5001",
			Cluster: &cluster.Config{
				Name: "dev",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"10.20.30.40:5000", "11.21.31.41:5001"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
				},
			},
		},
		{
			DSN: "hz://10.20.30.40:5000?cluster.name=my-cluster&unisocket=true",
			Cluster: &cluster.Config{
				Name: "my-cluster",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"10.20.30.40:5000"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
				},
				Unisocket: true,
			},
		},
		{
			DSN: "hz://10.20.30.40:5000?cluster.name=my-cluster&cloud.token=clfofaakEuQyCH4tANv863eOaa4GAi0arGqJjlzK7WfR9J8HkI",
			Cluster: &cluster.Config{
				Name: "my-cluster",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"10.20.30.40:5000"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
				},
				Cloud: cluster.CloudConfig{
					Token:   "clfofaakEuQyCH4tANv863eOaa4GAi0arGqJjlzK7WfR9J8HkI",
					Enabled: true,
				},
			},
		},
		{
			DSN: "hz://?log=error&ssl=true",
			Cluster: &cluster.Config{
				Name: "dev",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"127.0.0.1:5701"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
					SSL:               cluster.SSLConfig{Enabled: true},
				},
			},
			Logger: &logger.Config{Level: logger.ErrorLevel},
		},
		{
			DSN: "hz://someuser:@",
			Cluster: &cluster.Config{
				Name: "dev",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"127.0.0.1:5701"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
					SSL:               cluster.SSLConfig{Enabled: false},
				},
				Security: cluster.SecurityConfig{
					Credentials: cluster.CredentialsConfig{
						Username: "someuser",
					},
				},
			},
		},
		{
			DSN: "hz://someuser:somepass@foo.bar.com",
			Cluster: &cluster.Config{
				Name: "dev",
				Network: cluster.NetworkConfig{
					Addresses:         []string{"foo.bar.com"},
					PortRange:         cluster.PortRange{Min: 5701, Max: 5703},
					ConnectionTimeout: types.Duration(5 * time.Second),
					SSL:               cluster.SSLConfig{Enabled: false},
				},
				Security: cluster.SecurityConfig{
					Credentials: cluster.CredentialsConfig{
						Username: "someuser",
						Password: "somepass",
					},
				},
			},
		},
		{
			DSN: "localhost",
			Err: errors.New("parsing DSN: scheme is required"),
		},
		{
			DSN: "tcp://localhost",
			Err: errors.New("parsing DSN: unknown scheme: tcp"),
		},
		{
			DSN: "hz://localhost/some-path",
			Err: errors.New("parsing DSN: path is not allowed"),
		},
		{
			DSN: "hz://localhost/",
			Err: errors.New("parsing DSN: path is not allowed"),
		},
		{
			DSN: "hz://?ssl=yeah",
			Err: errors.New("parsing DSN options: invalid ssl option value: strconv.ParseBool: parsing \"yeah\": invalid syntax"),
		},
		{
			DSN: "hz://?ssl.ca.path=ca.pem",
			Err: errors.New("parsing DSN options: invalid ssl.cert.path value: illegal argument error"),
		},
		{
			DSN: "hz://?ssl.cert.path=cert.pem",
			Err: errors.New("parsing DSN options: invalid ssl.ca.path value: illegal argument error"),
		},
		{
			DSN: "hz://?ssl.cert.path=cert.pem",
			Err: errors.New("parsing DSN options: invalid ssl.ca.path value: illegal argument error"),
		},
		{
			DSN: "hz://?ssl.ca.path=ca.pem&ssl.cert.path=cert.pem",
			Err: errors.New("parsing DSN options: invalid ssl.key.path value: illegal argument error"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.DSN, func(t *testing.T) {
			if tc.Pre != nil {
				tc.Pre()
			}
			if tc.Cluster != nil {
				if err := tc.Cluster.Validate(); err != nil {
					t.Fatal(err)
				}
			}
			if tc.Logger != nil {
				if err := tc.Logger.Validate(); err != nil {
					t.Fatal(err)
				}
			}
			if tc.Serialization != nil {
				if err := tc.Serialization.Validate(); err != nil {
					t.Fatal(err)
				}
			}
			c, err := idriver.MakeConfigFromDSN(tc.DSN)
			if tc.Err != nil {
				if err == nil {
					t.Fatalf("expected error")
				}
				assert.Equal(t, tc.Err.Error(), err.Error())
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.Cluster != nil {
				assert.Equal(t, tc.Cluster, c.Cluster)
			}
			if tc.Logger != nil {
				assert.Equal(t, tc.Logger, c.Logger)
			}
			if tc.Serialization != nil {
				assert.Equal(t, tc.Serialization, c.Serialization)
			}
			if tc.Post != nil {
				tc.Post()
			}
		})
	}
}

type cursorBufferSizeTestCase struct {
	CtxFn  func() context.Context
	Name   string
	Target int32
	Panics bool
}

func TestExtractCursorBufferSize(t *testing.T) {
	testCases := []cursorBufferSizeTestCase{
		{
			Name:   "default",
			CtxFn:  func() context.Context { return context.Background() },
			Target: idriver.DefaultCursorBufferSize,
		},
		{
			Name:   "positive int32 size",
			CtxFn:  func() context.Context { return pubdriver.WithCursorBufferSize(context.Background(), 1000) },
			Target: 1000,
		},
		{
			Name:   "positive max int32 size",
			CtxFn:  func() context.Context { return pubdriver.WithCursorBufferSize(context.Background(), math.MaxInt32) },
			Target: math.MaxInt32,
		},
		{
			Name:   "negative size",
			CtxFn:  func() context.Context { return pubdriver.WithCursorBufferSize(context.Background(), -1) },
			Panics: true,
		},
		{
			Name:   "nil parent",
			CtxFn:  func() context.Context { return pubdriver.WithCursorBufferSize(nil, 1000) },
			Panics: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Panics {
				assert.Panics(t, func() { tc.CtxFn() })
				return
			}
			ctx := tc.CtxFn()
			assert.Equal(t, tc.Target, idriver.ExtractCursorBufferSize(ctx))
		})
	}
}

func TestExtractCursorBufferSize_Expect32Bit(t *testing.T) {
	skip.If(t, "arch ~ 32bit")
	v := math.MaxInt32
	assert.Panics(t, func() {
		pubdriver.WithCursorBufferSize(context.Background(), v+1)
	})
}

func TestExtractTimeoutMillis(t *testing.T) {
	testCases := []struct {
		CtxFn  func() context.Context
		Name   string
		Target int64
		Panics bool
	}{
		{
			Name:   "default",
			CtxFn:  func() context.Context { return context.Background() },
			Target: idriver.DefaultTimeoutMillis,
		},
		{
			Name:   "positive duration",
			CtxFn:  func() context.Context { return pubdriver.WithQueryTimeout(context.Background(), 10*time.Second) },
			Target: 10_000,
		},
		{
			Name:   "negative duration",
			CtxFn:  func() context.Context { return pubdriver.WithQueryTimeout(context.Background(), -1000) },
			Target: -1,
		},
		{
			Name:   "nil parent",
			CtxFn:  func() context.Context { return pubdriver.WithQueryTimeout(nil, 10*time.Second) },
			Panics: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Panics {
				assert.Panics(t, func() { tc.CtxFn() })
				return
			}
			ctx := tc.CtxFn()
			assert.Equal(t, tc.Target, idriver.ExtractTimeoutMillis(ctx))
		})
	}
}

func TestExtractSchema(t *testing.T) {
	testCases := []struct {
		CtxFn  func() context.Context
		Name   string
		Target string
	}{
		{
			Name:   "default",
			CtxFn:  func() context.Context { return context.Background() },
			Target: "",
		},
		{
			Name:   "with value",
			CtxFn:  func() context.Context { return pubdriver.WithSchema(context.Background(), "foo") },
			Target: "foo",
		},
		{
			Name:   "with blank value",
			CtxFn:  func() context.Context { return pubdriver.WithSchema(context.Background(), "") },
			Target: "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := tc.CtxFn()
			assert.Equal(t, tc.Target, idriver.ExtractSchema(ctx))
		})
	}
}
