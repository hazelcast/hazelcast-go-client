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

package cluster

import (
	"sync/atomic"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/security"
)

// FailoverService is responsible for cluster failover state and attempts management.
type FailoverService struct {
	clientRunningFn   func() bool
	candidateClusters []CandidateCluster
	maxTryCount       int
	index             uint64
}

type CandidateCluster struct {
	AddressProvider    AddressProvider
	AddressTranslator  AddressTranslator
	Credentials        security.Credentials
	ConnectionStrategy *pubcluster.ConnectionStrategyConfig
	ClusterName        string
}

type addrFun func(*pubcluster.Config, ilogger.Logger) (AddressProvider, AddressTranslator)

func NewFailoverService(logger ilogger.Logger, maxTries int, rootConfig pubcluster.Config, foConfigs []pubcluster.Config, addrFn addrFun, clientRunningFn func() bool) *FailoverService {
	candidates := []CandidateCluster{}
	configs := []pubcluster.Config{}
	if len(foConfigs) > 0 {
		configs = foConfigs
	} else {
		configs = append(configs, rootConfig)
	}
	for _, c := range configs {
		// copy c to a local variable in order to be able to take its address below
		cv := c
		cc := CandidateCluster{
			ClusterName:        c.Name,
			Credentials:        makeCredentials(&c.Security),
			ConnectionStrategy: &cv.ConnectionStrategy,
		}
		cc.AddressProvider, cc.AddressTranslator = addrFn(&c, logger)
		candidates = append(candidates, cc)
	}

	return &FailoverService{
		clientRunningFn:   clientRunningFn,
		maxTryCount:       maxTries,
		candidateClusters: candidates,
	}
}

func makeCredentials(config *pubcluster.SecurityConfig) *security.UsernamePasswordCredentials {
	return security.NewUsernamePasswordCredentials(config.Credentials.Username, config.Credentials.Password)
}

func (s *FailoverService) Current() *CandidateCluster {
	idx := atomic.LoadUint64(&s.index)
	return &s.candidateClusters[idx%uint64(len(s.candidateClusters))]
}

func (s *FailoverService) Next() {
	atomic.AddUint64(&s.index, 1)
}
