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

package statistics

import (
	"testing"

	"time"

	"strings"

	"os"

	"strconv"

	"log"

	"sort"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test/testutil"
	"github.com/stretchr/testify/assert"
)

const initialWait = 1 * time.Second

var remoteController rc.RemoteController

func TestMain(t *testing.M) {
	var err error
	remoteController, err = rc.NewRemoteControllerClient("localhost:9701")
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	t.Run()
}

func TestClientStatisticsDisabledByDefault(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)

	client, _ := hazelcast.NewClient()
	defer client.Shutdown()

	time.Sleep(initialWait)

	assert.Equalf(t, len(GetClientStatsFromServer(t, cluster.ID)), 0, "Statistics"+
		"should be disabled by default.")

}

func TestClientStatisticsDisabledWithWrongValue(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)

	config := hazelcast.NewConfig()
	config.SetProperty(property.StatisticsEnabled.Name(), "trueee")

	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()

	time.Sleep(initialWait)

	assert.Equalf(t, len(GetClientStatsFromServer(t, cluster.ID)), 0, "Statistics"+
		"should not be enabled with wrong value.")

}

func TestClientStatisticsEnabled(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)

	config := hazelcast.NewConfig()
	config.SetProperty(property.StatisticsEnabled.Name(), "true")

	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()

	time.Sleep(initialWait)

	if len(GetClientStatsFromServer(t, cluster.ID)) == 0 {
		t.Errorf("Statistics should be enabled")
	}
}

func TestClientStatisticsEnabledViaEnvironmentVariable(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)

	config := hazelcast.NewConfig()
	os.Setenv(property.StatisticsEnabled.Name(), "true")
	defer os.Remove(property.StatisticsEnabled.Name())

	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()

	time.Sleep(initialWait)

	if len(GetClientStatsFromServer(t, cluster.ID)) == 0 {
		t.Errorf("Statistics should be enabled")
	}
}

func TestClientStatisticsPeriod(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)

	config := hazelcast.NewConfig()
	config.SetProperty(property.StatisticsEnabled.Name(), "true")

	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()

	period, _ := strconv.Atoi(property.StatisticsPeriodSeconds.DefaultValue())

	time.Sleep(time.Duration(period+1) * time.Second)

	stats1 := GetClientStatsFromServer(t, cluster.ID)
	if len(stats1) == 0 {
		t.Fatal("Statistics should have returned non-empty string.")
	}

	time.Sleep(time.Duration(period+1) * time.Second)

	stats2 := GetClientStatsFromServer(t, cluster.ID)
	if len(stats2) == 0 {
		t.Fatal("Statistics should have returned non-empty string.")
	}

	if strings.Compare(stats1, stats2) == 0 {
		t.Errorf("Initial statistics should not be same with next collected statistics %s %s",
			stats1, stats2)
	}

}

func TestClientStatisticsNegativePeriodShouldSendStatistics(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)

	config := hazelcast.NewConfig()
	config.SetProperty(property.StatisticsEnabled.Name(), "true")
	config.SetProperty(property.StatisticsPeriodSeconds.Name(), "-1")

	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()

	testutil.AssertTrueEventually(t, func() bool {
		stats1 := GetClientStatsFromServer(t, cluster.ID)
		return len(stats1) > 0
	})
}

func TestClientStatisticsNonDefaultPeriod(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)

	config := hazelcast.NewConfig()
	config.SetProperty(property.StatisticsEnabled.Name(), "true")
	config.SetProperty(property.StatisticsPeriodSeconds.Name(), "3")
	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()

	time.Sleep(time.Duration(1) * time.Second)

	stats1 := GetClientStatsFromServer(t, cluster.ID)
	if len(stats1) == 0 {
		t.Fatal("Statistics should have returned non-empty string.")
	}

	time.Sleep(time.Duration(1) * time.Second)

	stats2 := GetClientStatsFromServer(t, cluster.ID)
	if len(stats2) == 0 {
		t.Fatal("Statistics should have returned non-empty string.")
	}

	if strings.Compare(stats1, stats2) != 0 {
		t.Errorf("Statistics should not be updated before period %s %s",
			stats1, stats2)
	}

	time.Sleep(time.Duration(5) * time.Second)

	stats3 := GetClientStatsFromServer(t, cluster.ID)
	if len(stats3) == 0 {
		t.Fatal("Statistics should have returned non-empty string.")
	}

	if strings.Compare(stats3, stats2) == 0 {
		t.Errorf("Statistics should not be same %s %s",
			stats1, stats2)
	}

}

func TestClientStatisticsContent(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)

	config := hazelcast.NewConfig()
	config.SetProperty(property.StatisticsEnabled.Name(), "true")

	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()

	time.Sleep(initialWait)

	res := GetClientStatsFromServer(t, cluster.ID)
	if len(res) == 0 {
		t.Fatal("Statistics should have returned non-empty string.")
	}
	assert.Equal(t, true, strings.Contains(res, "clientName="+client.Name()))
	assert.Equal(t, true, strings.Contains(res, "lastStatisticsCollectionTime="))
	assert.Equal(t, true, strings.Contains(res, "enterprise=false"))
	assert.Equal(t, true, strings.Contains(res, "clientType=GO"))
	assert.Equal(t, true, strings.Contains(res, "clientVersion="+internal.ClientVersion))
	assert.Equal(t, true, strings.Contains(res, "clusterConnectionTimestamp="))
	assert.Equal(t, true, strings.Contains(res, "clientAddress="))
	assert.Equal(t, true, strings.Contains(res, "os.maxFileDescriptorCount="))
	assert.Equal(t, true, strings.Contains(res, "os.openFileDescriptorCount="))
	assert.Equal(t, true, strings.Contains(res, "runtime.availableProcessors="))
	assert.Equal(t, true, strings.Contains(res, "runtime.freeMemory="))
	assert.Equal(t, true, strings.Contains(res, "runtime.totalMemory="))
	assert.Equal(t, true, strings.Contains(res, "runtime.usedMemory="))

}

func TestClientStatisticsContentChanges(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)

	config := hazelcast.NewConfig()
	config.SetProperty(property.StatisticsEnabled.Name(), "true")
	config.SetProperty(property.StatisticsPeriodSeconds.Name(), "1")

	client, _ := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	var stats string
	testutil.AssertTrueEventually(t, func() bool {
		stats = GetClientStatsFromServer(t, cluster.ID)
		return len(stats) > 0
	})

	runTimeMetrics := getRuntimeMetrics(stats)
	testutil.AssertTrueEventually(t, func() bool {
		nextStats := GetClientStatsFromServer(t, cluster.ID)
		if stats == nextStats {
			return false
		}
		nextRuntimeMetrics := getRuntimeMetrics(nextStats)
		return isRuntimeMetricsDifferent(runTimeMetrics, nextRuntimeMetrics)
	})

}

func isRuntimeMetricsDifferent(runtimeMetrics []string, nextRuntimeMetrics []string) bool {
	if len(runtimeMetrics) != len(nextRuntimeMetrics) {
		return false
	}
	for index, nextRuntimeMetric := range nextRuntimeMetrics {
		if runtimeMetrics[index] != nextRuntimeMetric {
			return true
		}
	}
	return false
}

func getRuntimeMetrics(stats string) []string {
	metrics := strings.Split(stats, ",")
	runtimeMetrics := make([]string, 0)
	for _, metric := range metrics {
		if strings.Contains(metric, "runtime.") {
			runtimeMetrics = append(runtimeMetrics, metric)
		}
	}
	sort.Strings(runtimeMetrics)
	return runtimeMetrics

}

func GetClientStatsFromServer(t *testing.T, clusterID string) string {
	script := "client0=instance_0.getClientService().getConnectedClients()." +
		"toArray()[0]\nresult=client0.getClientStatistics();"
	response, err := remoteController.ExecuteOnController(clusterID, script, rc.Lang_PYTHON)
	if err != nil || !response.Success {
		t.Fatal("Failed to gather statistics from server:", err, response.String())
	}
	return string(response.GetResult_())
}
