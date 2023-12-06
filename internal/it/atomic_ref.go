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
	"github.com/hazelcast/hazelcast-go-client/internal/cp"
)

type AtomicRefTestContext struct {
	T       *testing.T
	A       *cp.AtomicRef
	Cluster *TestCluster
	Client  *hz.Client
}

func (tcx *AtomicRefTestContext) Tester(f func(tcx *AtomicRefTestContext)) {
	ctx := context.Background()
	defer func() {
		if tcx.A != nil {
			// ignoring the error here
			_ = tcx.A.Destroy(ctx)
		}
		if tcx.Client != nil {
			// ignoring the error here
			_ = tcx.Client.Shutdown(ctx)
		}
	}()
	ensureRemoteController(true)
	tcx.Cluster = defaultTestCluster.Launch(tcx.T)
	config := tcx.Cluster.DefaultConfig()
	if tcx.Client == nil {
		tcx.Client = getDefaultClient(&config)
	}
	name := NewUniqueObjectName("atomicref")
	ar, err := tcx.Client.CPSubsystem().GetAtomicRef(ctx, name)
	if err != nil {
		panic(err)
	}
	tcx.A = ar
	tcx.T.Logf("AtomicRef name: %s", tcx.A.Name())
	f(tcx)
}
