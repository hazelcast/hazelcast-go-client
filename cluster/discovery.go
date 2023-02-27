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

package cluster

import (
	"context"

	"github.com/hazelcast/hazelcast-go-client/logger"
)

type DiscoveryNode struct {
	PublicAddr  Address
	PrivateAddr Address
}

type DiscoveryStrategyOptions struct {
	Logger      logger.Logger
	UsePublicIP bool
}

type DiscoveryStrategyStarter interface {
	Start(ctx context.Context, opts DiscoveryStrategyOptions) error
}

type DiscoveryStrategyDestroyer interface {
	Destroy(ctx context.Context) error
}

type DiscoveryStrategy interface {
	DiscoverNodes(ctx context.Context) ([]DiscoveryNode, error)
}
