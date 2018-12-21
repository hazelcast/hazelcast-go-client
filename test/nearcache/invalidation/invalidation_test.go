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

package invalidation

import (
	"testing"

	"log"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/hazelcast/hazelcast-go-client/test/nearcache"
)

var remoteController rc.RemoteController

func TestMain(m *testing.M) {
	var err error
	remoteController, err = rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	m.Run()
}

func configureConfig(config *config.Config) {
	config.SetProperty(property.MaxToleratedMissCount.Name(), "0")
	config.SetProperty(property.ReconciliationIntervalSeconds.Name(), "10")
	config.SetProperty(property.MinReconciliationIntervalSeconds.Name(), "10")
}
func TestSequenceFixIfKeyRemoveAtServer(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	remoteController.StartMember(cluster.ID)

	theKey := "key"

	config := nearcache.CreateConfigWithDefaultNearCache()
	configureConfig(config)

	client, _ := hazelcast.NewClientWithConfig(config)
	mp, _ := client.GetMap("name")

	mp.Put(theKey, "value1")
	cache := nearcache.GetNearCacheFromMap(mp)

	cache.

}
