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

package discovery

import (
	"context"

	"github.com/hazelcast/hazelcast-go-client/logger"
)

// Node is a public-private address pair.
type Node struct {
	PublicAddr  string
	PrivateAddr string
}

// StrategyOptions are passed to the Strategy's Start method.
type StrategyOptions struct {
	Logger      logger.Logger
	UsePublicIP bool
}

// StrategyStarter is an optional interface for a discovery strategy.
// If implemented, Start will be called before DiscoverNodes method.
// Note that it may be called more than once if the previous start was not successful.
type StrategyStarter interface {
	Start(ctx context.Context, opts StrategyOptions) error
}

// StrategyDestroyer is an optional interface for a discovery strategy.
// If implemented, Destroy method will be called once during client shutdown.
type StrategyDestroyer interface {
	Destroy(ctx context.Context) error
}

// Strategy is the discovery strategy interface.
// DiscoverNodes may be called several times.
type Strategy interface {
	DiscoverNodes(ctx context.Context) ([]Node, error)
}
