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
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
)

func SerializationTester(t *testing.T, f func(t *testing.T, config hazelcast.Config, clusterID, mapName string)) {
	ensureRemoteController(true)
	mapName := NewUniqueObjectName("map")
	cls := defaultTestCluster.Launch(t)
	config := cls.DefaultConfig()
	f(t, config, cls.ClusterID, mapName)
}
