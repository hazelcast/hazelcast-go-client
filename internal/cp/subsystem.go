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

package cp

import (
	"context"

	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

/*
Subsystem represent the CP service.
CP Subsystem is a component of Hazelcast that builds a strongly consistent layer for a set of distributed data structures.
Its APIs can be used for implementing distributed coordination use cases, such as leader election, distributed locking, synchronization, and metadata management.
Its data structures are CP with respect to the CAP principle, i.e., they always maintain linearizability and prefer consistency over availability during network partitions.
Besides network partitions, CP Subsystem withstands server and client failures.
Data structures in CP Subsystem run in CP groups.
Each CP group elects its own Raft leader and runs the Raft consensus algorithm independently.
The CP data structures differ from the other Hazelcast data structures in two aspects.
First, an internal commit is performed on the METADATA CP group every time you fetch a proxy from this interface.
Hence, callers should cache returned proxy objects.
Second, if you call "destroy()" on a CP data structure proxy, that data structure is terminated on the underlying CP group and cannot be reinitialized until the CP group is force-destroyed.
For this reason, please make sure that you are completely done with a CP data structure before destroying its proxy.
*/
type Subsystem struct {
	proxyFactory *proxyFactory
}

func NewSubsystem(ss *iserialization.Service, cif *cluster.ConnectionInvocationFactory, is *invocation.Service, l *logger.LogAdaptor) Subsystem {
	return Subsystem{
		proxyFactory: newProxyFactory(ss, cif, is, l),
	}
}

// GetAtomicLong returns the distributed AtomicLong instance with given name.
func (c Subsystem) GetAtomicLong(ctx context.Context, name string) (*AtomicLong, error) {
	return c.proxyFactory.getAtomicLong(ctx, name)
}

// GetAtomicRef returns the distributed AtomicRef instance with given name.
func (c Subsystem) GetAtomicRef(ctx context.Context, name string) (*AtomicRef, error) {
	return c.proxyFactory.getAtomicRef(ctx, name)
}

// GetMap returns the distributed CPMap instance with given name.
func (c Subsystem) GetMap(ctx context.Context, name string) (*Map, error) {
	return c.proxyFactory.getMap(ctx, name)
}
