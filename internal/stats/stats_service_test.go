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

type Handler struct {
	okCh chan struct{}
}

func (h Handler) Invoke(invocation invocation.Invocation) (groupID int64, err error) {
	h.okCh <- struct{}{}
	return 1, nil
}

func TestNewService(t *testing.T) {
	lg := logger.New()
	ed := event.NewDispatchService(lg)
	okCh := make(chan struct{}, 1)
	handler := Handler{okCh: okCh}
	config := hazelcast.Config{}
	invService := invocation.NewService(handler, ed, lg, &config.Cluster.Event)
	invFac := cluster.NewConnectionInvocationFactory(&config.Cluster)
	srv := stats.NewService(invService, invFac, ed, lg, 100*time.Millisecond, "hz1")
	srv.Start()
	address := pubcluster.NewAddress("100.200.300.400", 12345)
	ed.Publish(cluster.NewConnected(address))
	select {
	case _, ok := <-okCh:
		// TODO: decode the request and check whether it's correct.
		srv.Stop()
		if ok != true {
			t.Fatalf("an invocation was expected")
		}
	case <-time.After(2 * time.Minute):
		srv.Stop()
		t.Fatalf("invocation not received in time")
	}
}
