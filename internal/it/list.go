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

func ListTester(t *testing.T, f func(t *testing.T, l *hz.List)) {
	makeListName := func() string {
		return fmt.Sprintf("test-list-%d-%d", idGen.NextID(), rand.Int())
	}
	ListTesterWithConfigAndName(t, makeListName, nil, f)
}

func ListTesterWithConfigAndName(t *testing.T, listName func() string, cbCallback func(*hz.Config), f func(*testing.T, *hz.List)) {
	var (
		client *hz.Client
		l      *hz.List
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
		config.Cluster.SmartRouting = smart
		client, l = getClientListWithConfig(listName(), &config)
		defer func() {
			if err := l.Destroy(context.Background()); err != nil {
				t.Logf("test warning, could not destroy list: %s", err.Error())
			}
			if err := client.Shutdown(); err != nil {
				t.Logf("test warning, client not shutdown: %s", err.Error())
			}
		}()
		f(t, l)
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

func getClientListWithConfig(name string, config *hz.Config) (*hz.Client, *hz.List) {
	client := getDefaultClient(config)
	if l, err := client.GetList(context.Background(), name); err != nil {
		panic(err)
	} else {
		return client, l
	}
}
