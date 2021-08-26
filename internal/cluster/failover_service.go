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
	AddressProvider   AddressProvider
	AddressTranslator AddressTranslator
	Credentials       security.Credentials
	ClusterName       string
}

func NewFailoverService(
	logger ilogger.Logger,
	maxTryCount int,
	rootConfig pubcluster.Config,
	failoverConfigs []pubcluster.Config,
	addrProviderTranslatorFn func(*pubcluster.Config, ilogger.Logger) (AddressProvider, AddressTranslator),
	isClientRunningFn func() bool) *FailoverService {

	candidateClusters := []CandidateCluster{}
	configs := []pubcluster.Config{rootConfig}
	configs = append(configs, failoverConfigs...)
	for _, c := range configs {
		ctx := CandidateCluster{
			ClusterName: c.Name,
			Credentials: makeCredentials(&c.Security),
		}
		ctx.AddressProvider, ctx.AddressTranslator = addrProviderTranslatorFn(&c, logger)
		candidateClusters = append(candidateClusters, ctx)
	}

	return &FailoverService{
		clientRunningFn:   isClientRunningFn,
		maxTryCount:       maxTryCount,
		candidateClusters: candidateClusters,
	}
}

func makeCredentials(config *pubcluster.SecurityConfig) *security.UsernamePasswordCredentials {
	return security.NewUsernamePasswordCredentials(config.Credentials.Username, config.Credentials.Password)
}

func (s *FailoverService) TryNextCluster(fn func(next *CandidateCluster) (pubcluster.Address, bool)) (pubcluster.Address, bool) {
	tryCount := 0
	for s.clientRunningFn() && tryCount < s.maxTryCount {
		for i := 0; i < len(s.candidateClusters); i++ {
			if addr, connected := fn(s.Next()); connected {
				return addr, true
			}
		}
		tryCount++
	}
	return "", false
}

func (s *FailoverService) Current() *CandidateCluster {
	idx := atomic.LoadUint64(&s.index)
	return &s.candidateClusters[idx%uint64(len(s.candidateClusters))]
}

func (s *FailoverService) Next() *CandidateCluster {
	idx := atomic.AddUint64(&s.index, 1)
	return &s.candidateClusters[idx%uint64(len(s.candidateClusters))]
}
