/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

	hz "github.com/hazelcast/hazelcast-go-client"
)

func AtomicLongTester(t *testing.T, f func(t *testing.T, a *hz.AtomicLong)) {
	AtomicLongTesterWithConfig(t, nil, f)
}

func AtomicLongTesterWithConfig(t *testing.T, configCallback func(*hz.Config), f func(t *testing.T, a *hz.AtomicLong)) {
	makeName := func() string {
		return NewUniqueObjectName("atomic-long")
	}
	AtomicLongTesterWithConfigAndName(t, makeName, configCallback, f)
}

func AtomicLongTesterWithConfigAndName(t *testing.T, makeName func() string, configCallback func(*hz.Config), f func(t *testing.T, a *hz.AtomicLong)) {
	ensureRemoteController(true)
	runner := func(t *testing.T, smart bool) {
		cls := cpEnabledTestCluster.Launch(t)
		config := cls.DefaultConfig()
		if configCallback != nil {
			configCallback(&config)
		}
		config.Cluster.Unisocket = !smart
		client, atm := GetClientAtomicLongWithConfig(makeName(), &config)
		defer func() {
			ctx := context.Background()
			if err := atm.Destroy(ctx); err != nil {
				t.Logf("test warning, could not destroy atomic long conter: %s", err.Error())
			}
			if err := client.Shutdown(ctx); err != nil {
				t.Logf("Test warning, client not shutdown: %s", err.Error())
			}
		}()
		f(t, atm)
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

func GetClientAtomicLongWithConfig(name string, config *hz.Config) (*hz.Client, *hz.AtomicLong) {
	client := getDefaultClient(config)
	cp := client.CPSubsystem()
	al, err := cp.GetAtomicLong(context.Background(), name)
	if err != nil {
		panic(err)
	}
	return client, al
}
