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

package driver_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/it/runtime"
	"github.com/hazelcast/hazelcast-go-client/internal/sql/driver"
	pubdriver "github.com/hazelcast/hazelcast-go-client/sql/driver"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestParseDSN(t *testing.T) {
	testCases := []struct {
		Cluster *cluster.Config
		Err     error
		DSN     string
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
			DSN: "hz://10.20.30.40:5000?cluster.name=my-cluster&cluster.unisocket=true",
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
	}
	for _, tc := range testCases {
		t.Run(tc.DSN, func(t *testing.T) {
			if tc.Cluster != nil {
				if err := tc.Cluster.Validate(); err != nil {
					t.Fatal(err)
				}
			}
			c, err := driver.MakeConfigFromDSN(tc.DSN)
			if tc.Err != nil {
				if err == nil {
					t.Fatalf("expected error")
				}
				assert.Equal(t, tc.Err.Error(), err.Error())
			} else if err != nil {
				t.Fatal(err)
			} else {
				assert.Equal(t, tc.Cluster, c.Cluster)
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
			Target: driver.DefaultCursorBufferSize,
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
			Name:   "zero size",
			CtxFn:  func() context.Context { return pubdriver.WithCursorBufferSize(context.Background(), 0) },
			Panics: true,
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
	if !runtime.Is32BitArch() {
		v := math.MaxInt32
		testCases = append(testCases, cursorBufferSizeTestCase{
			Name:   "> 32bit",
			CtxFn:  func() context.Context { return pubdriver.WithCursorBufferSize(context.Background(), v+1) },
			Panics: true,
		})
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Panics {
				assert.Panics(t, func() { tc.CtxFn() })
				return
			}
			ctx := tc.CtxFn()
			assert.Equal(t, tc.Target, driver.ExtractCursorBufferSize(ctx))
		})
	}
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
			Target: driver.DefaultTimeoutMillis,
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
			assert.Equal(t, tc.Target, driver.ExtractTimeoutMillis(ctx))
		})
	}
}
