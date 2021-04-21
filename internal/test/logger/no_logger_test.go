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
	"bytes"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/core/logger"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/test/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNoLogger(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", testutil.DefaultServerConfig)
	defer remoteController.ShutdownCluster(cluster.ID)
	remoteController.StartMember(cluster.ID)
	buf := new(bytes.Buffer)
	l := logger.New()
	l.SetOutput(buf)
	level, _ := logger.GetLogLevel(logger.OffLevel)
	l.Level = level
	config := hazelcast.NewConfig()
	config.LoggerConfig().SetLogger(l)
	client, err := hazelcast.NewClientWithConfig(config)
	assert.NoError(t, err)
	assert.Zero(t, buf.Len())
	client.Shutdown()
}
