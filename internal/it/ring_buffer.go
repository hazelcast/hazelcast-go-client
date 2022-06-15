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

package it

import (
	"context"
	"testing"

	"go.uber.org/goleak"

	hz "github.com/hazelcast/hazelcast-go-client"
)

func RingbufferTester(t *testing.T, f func(t *testing.T, rb *hz.Ringbuffer)) {
	name := func() string {
		return NewUniqueObjectName("ringbuffer")
	}
	RingbufferTesterWithConfigAndName(t, name, nil, f)
}

func RingbufferTesterWithConfigAndName(t *testing.T, ringBufferName func() string, cbCallback func(*hz.Config), f func(*testing.T, *hz.Ringbuffer)) {
	var (
		client *hz.Client
		rb     *hz.Ringbuffer
	)
	ensureRemoteController(true)
	runner := func(t *testing.T, smart bool) {
		if LeakCheckEnabled() {
			t.Logf("enabled leak check")
			defer goleak.VerifyNone(t)
		}
		config := defaultTestCluster.DefaultConfig()
		if cbCallback != nil {
			cbCallback(&config)
		}
		config.Cluster.Unisocket = !smart
		client, rb = getClientRingbufferWithConfig(ringBufferName(), &config)
		defer func() {
			ctx := context.Background()
			if err := rb.Destroy(ctx); err != nil {
				t.Logf("test warning, could not destroy ringbuffer: %s", err.Error())
			}
			if err := client.Shutdown(ctx); err != nil {
				t.Logf("test warning, client not shutdown: %s", err.Error())
			}
		}()
		f(t, rb)
	}
	if SmartEnabled() {
		t.Run("Smart Client", func(t *testing.T) {
			runner(t, true)
		})
	}
	if NonSmartEnabled() {
		t.Run("Non-Smart Client", func(t *testing.T) {
			runner(t, false)
		})
	}
}

func getClientRingbufferWithConfig(name string, config *hz.Config) (*hz.Client, *hz.Ringbuffer) {
	client := getDefaultClient(config)
	if l, err := client.GetRingbuffer(context.Background(), name); err != nil {
		panic(err)
	} else {
		return client, l
	}
}
