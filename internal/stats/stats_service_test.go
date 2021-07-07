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

package stats_test

import (
	"testing"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/stats"
)

func TestNewService(t *testing.T) {
	ed := event.NewDispatchService()
	requestCh := make(chan invocation.Invocation, 1)
	lg := logger.New()
	config := hazelcast.NewConfig()
	invFac := cluster.NewConnectionInvocationFactory(&config.Cluster)
	srv := stats.NewService(requestCh, invFac, ed, lg, 100*time.Millisecond, "hz1")
	srv.Start()
	ed.Publish(cluster.NewConnected(pubcluster.NewAddress("100.200.300.400", 12345)))
	time.Sleep(100 * time.Millisecond)
	srv.Stop()
	select {
	case _, ok := <-requestCh:
		// TODO: decode the request and check whether it's correct.
		if ok != true {
			t.Fatalf("an invocation was expected")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("invocation not received in time")
	}
}
