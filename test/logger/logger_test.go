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

package logger

import (
	"os"
	"testing"

	"log"

	"bytes"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test"
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

func TestLoggerConfigurationWithEnvironmentVariable(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	os.Setenv(property.LoggingLevel.Name(), logger.ErrorLevel)
	defer os.Unsetenv(property.LoggingLevel.Name())
	l := logger.New()
	buf := new(bytes.Buffer)
	l.SetOutput(buf)
	config := hazelcast.NewConfig()
	config.LoggerConfig().SetLogger(l)
	client, err := hazelcast.NewClientWithConfig(config)
	assert.NoError(t, err)
	client.Shutdown()
	assert.Zero(t, buf.Len())
}

func TestLoggerLevelConfigurationWithConfig(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	l := logger.New()
	buf := new(bytes.Buffer)
	l.SetOutput(buf)
	config := hazelcast.NewConfig()
	config.SetProperty(property.LoggingLevel.Name(), logger.ErrorLevel)
	config.LoggerConfig().SetLogger(l)
	client, err := hazelcast.NewClientWithConfig(config)
	assert.NoError(t, err)
	client.Shutdown()
	assert.Zero(t, buf.Len())
}

func TestDefaultLogger(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
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
	cluster, _ := remoteController.CreateCluster("", test.DefaultServerConfig)
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

	log.Println(logMessage)
	assert.Contains(t, logMessage, clientName)
	assert.Contains(t, logMessage, groupName)
	assert.Contains(t, logMessage, "INFO")
}
