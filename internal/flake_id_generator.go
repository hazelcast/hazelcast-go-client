// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"github.com/hazelcast/hazelcast-go-client/internal/flake_id"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

type FlakeIdGeneratorProxy struct {
	*proxy
	batcher *flake_id.AutoBatcher
}

func (self *FlakeIdGeneratorProxy) NewId() (id int64, err error) {
	return self.batcher.NewId()
}

func (self *FlakeIdGeneratorProxy) NewIdBatch(batchSize int32) (*flake_id.IdBatch, error) {
	request := protocol.FlakeIdGeneratorNewIdBatchEncodeRequest(self.name, batchSize)
	responseMessage, err := self.invokeOnRandomTarget(request)
	if err != nil {
		return nil, err
	}
	base, increment, newBatchSize := protocol.FlakeIdGeneratorNewIdBatchDecodeResponse(responseMessage)()
	return flake_id.NewIdBatch(base, increment, newBatchSize), nil
}

func newFlakeIdGenerator(client *HazelcastClient, serviceName *string, name *string) (*FlakeIdGeneratorProxy, error) {
	config := client.ClientConfig.GetFlakeIdGeneratorConfig(*name)
	flakeIdGenerator := &FlakeIdGeneratorProxy{}
	flakeIdGenerator.proxy = &proxy{client: client, serviceName: serviceName, name: name}
	flakeIdGenerator.batcher = flake_id.NewAutoBatcher(config.PrefetchCount(), config.PrefetchValidityMillis(), flakeIdGenerator)
	return flakeIdGenerator, nil
}
