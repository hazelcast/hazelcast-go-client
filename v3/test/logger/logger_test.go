// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package logger

import (
	"testing"

	"log"

	"bytes"

	"github.com/hazelcast/hazelcast-go-client/v3"
	"github.com/hazelcast/hazelcast-go-client/v3/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v3/rc"
	"github.com/hazelcast/hazelcast-go-client/v3/test/testutil"
	"github.com/stretchr/testify/assert"
)

var remoteController *rc.RemoteControllerClient

func TestMain(m *testing.M) {
	rc, err := rc.NewRemoteControllerClient("localhost:9701")
	remoteController = rc
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	m.Run()
}

func TestLoggerLevelConfigurationWithConfig(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	l := logger.New()
	buf := new(bytes.Buffer)
	l.SetOutput(buf)
	config := hazelcast.NewConfig()
	level, _ := logger.GetLogLevel(logger.ErrorLevel)
	l.Level = level
	config.LoggerConfig().SetLogger(l)
	client, err := hazelcast.NewClientWithConfig(config)
	assert.NoError(t, err)
	client.Shutdown()
	assert.Zero(t, buf.Len())
}

func TestLoggerLevelWhenSetLoggerIsUsed(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)

	config := hazelcast.NewConfig()

	// Turn off logging
	hazelcastDefaultLogger := logger.New()
	buf := new(bytes.Buffer)
	hazelcastDefaultLogger.SetOutput(buf)
	offLogLevel, _ := logger.GetLogLevel(logger.OffLevel)
	hazelcastDefaultLogger.Level = offLogLevel
	config.LoggerConfig().SetLogger(hazelcastDefaultLogger)

	// Create client
	client, err := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	assert.NoError(t, err)
	assert.Zero(t, buf.Len())
}

func TestDefaultLogger(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	l := logger.New()
	buf := new(bytes.Buffer)
	l.SetOutput(buf)
	config := hazelcast.NewConfig()
	config.LoggerConfig().SetLogger(l)
	client, err := hazelcast.NewClientWithConfig(config)
	assert.NoError(t, err)
	client.Shutdown()
	assert.NotZero(t, buf.Len())
}

func TestDefaultLoggerContent(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	l := logger.New()
	buf := new(bytes.Buffer)
	l.SetOutput(buf)
	config := hazelcast.NewConfig()
	config.LoggerConfig().SetLogger(l)
	client, err := hazelcast.NewClientWithConfig(config)
	assert.NoError(t, err)

	clientName := client.Name()
	groupName := config.GroupConfig().Name()

	client.Shutdown()
	logMessage := buf.String()

	assert.Contains(t, logMessage, clientName)
	assert.Contains(t, logMessage, groupName)
	assert.Contains(t, logMessage, "INFO")
}

func TestDefaultLoggerFormat(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	l := logger.New()
	buf := new(bytes.Buffer)
	l.SetOutput(buf)
	config := hazelcast.NewConfig()
	config.LoggerConfig().SetLogger(l)
	client, err := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	assert.NoError(t, err)

	logMessage := buf.String()
	dateRegex := "\\d{1,4}/\\d{1,4}/\\d{1,4}"
	timeRegex := "\\d{1,4}:\\d{1,4}:\\d{1,4}"
	functionNameRegex := "github.com/hazelcast.*"
	clientNameRegex := "hz.client_\\d+"
	groupNameRegex := "\\[" + config.GroupConfig().Name() + "\\]"
	clientVersionRegex := "\\[\\d+.\\d+(-SNAPSHOT)?\\]"
	messageRegex := ".+"
	assert.Regexp(t, "^"+dateRegex+" "+timeRegex+" "+functionNameRegex+"\nINFO: "+clientNameRegex+
		" "+groupNameRegex+" "+clientVersionRegex+" "+messageRegex+"", logMessage)
}
