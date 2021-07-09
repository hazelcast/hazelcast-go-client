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

package it

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"go.uber.org/goleak"

	hz "github.com/hazelcast/hazelcast-go-client"
)

func SetTester(t *testing.T, f func(t *testing.T, s *hz.Set)) {
	SetTesterWithConfig(t, nil, f)
}

func SetTesterWithConfig(t *testing.T, configCallback func(*hz.Config), f func(t *testing.T, s *hz.Set)) {
	makeName := func() string {
		return fmt.Sprintf("test-set-%d-%d", idGen.NextID(), rand.Int())
	}
	SetTesterWithConfigAndName(t, makeName, configCallback, f)
}

func SetTesterWithConfigAndName(t *testing.T, makeName func() string, configCallback func(*hz.Config), f func(t *testing.T, s *hz.Set)) {
	var (
		client *hz.Client
		s      *hz.Set
	)
	ensureRemoteController(true)
	runner := func(t *testing.T, smart bool) {
		if LeakCheckEnabled() {
			t.Logf("enabled leak check")
			defer goleak.VerifyNone(t)
		}
		config := defaultTestCluster.DefaultConfig()
		if configCallback != nil {
			configCallback(&config)
		}
		config.Cluster.Unisocket = !smart
		client, s = GetClientSetWithConfig(makeName(), &config)
		defer func() {
			ctx := context.Background()
			if err := s.Destroy(ctx); err != nil {
				t.Logf("test warning, could not destroy set: %s", err.Error())
			}
			if err := client.Shutdown(ctx); err != nil {
				t.Logf("Test warning, client not shutdown: %s", err.Error())
			}
		}()
		f(t, s)
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

func GetClientSetWithConfig(setName string, config *hz.Config) (*hz.Client, *hz.Set) {
	client := getDefaultClient(config)
	if s, err := client.GetSet(context.Background(), setName); err != nil {
		panic(err)
	} else {
		return client, s
	}
}
