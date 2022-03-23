//go:build hazelcastinternal && hazelcastinternaltest
// +build hazelcastinternal,hazelcastinternaltest

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

package hazelcast

import (
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
)

func (ci *ClientInternal) ConnectionManager() *cluster.ConnectionManager {
	return ci.client.ic.ConnectionManager
}

func (ci *ClientInternal) DispatchService() *event.DispatchService {
	return ci.client.ic.EventDispatcher
}

func (ci *ClientInternal) InvocationService() *invocation.Service {
	return ci.client.ic.InvocationService
}

func (ci *ClientInternal) InvocationHandler() invocation.Handler {
	return ci.client.ic.InvocationHandler
}

func (ci *ClientInternal) ClusterService() *cluster.Service {
	return ci.client.ic.ClusterService
}
