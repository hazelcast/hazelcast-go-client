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

	hz "github.com/hazelcast/hazelcast-go-client"
)

func PNCounterTester(t *testing.T, f func(t *testing.T, pn *hz.PNCounter)) {
	PNCounterTesterWithConfig(t, nil, f)
}

func PNCounterTesterWithConfig(t *testing.T, configCallback func(*hz.Config), f func(t *testing.T, pn *hz.PNCounter)) {
	makeName := func() string {
		return fmt.Sprintf("test-pn-counter-%d-%d", idGen.NextID(), rand.Int())
	}
	PNCounterTesterWithConfigAndName(t, makeName, configCallback, f)
}

func PNCounterTesterWithConfigAndName(t *testing.T, makeName func() string, configCallback func(*hz.Config), f func(t *testing.T, s *hz.PNCounter)) {
	var (
		client *hz.Client
		pn     *hz.PNCounter
	)
	ensureRemoteController(true)
	runner := func(t *testing.T, smart bool) {
		config := defaultTestCluster.DefaultConfig()
		if configCallback != nil {
			configCallback(&config)
		}
		config.Cluster.Unisocket = !smart
		client, pn = GetClientPNCounterWithConfig(makeName(), &config)
		defer func() {
			if err := pn.Destroy(context.Background()); err != nil {
				t.Logf("test warning, could not destroy pn conter: %s", err.Error())
			}
			if err := client.Shutdown(); err != nil {
				t.Logf("Test warning, client not shutdown: %s", err.Error())
			}
		}()
		f(t, pn)
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

func GetClientPNCounterWithConfig(name string, config *hz.Config) (*hz.Client, *hz.PNCounter) {
	client := getDefaultClient(config)
	if pn, err := client.GetPNCounter(context.Background(), name); err != nil {
		panic(err)
	} else {
		return client, pn
	}
}
