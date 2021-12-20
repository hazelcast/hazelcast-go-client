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

package stats

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
)

type stat struct {
	k string
	v string
}

type binTextStats struct {
	mc    *MetricsCompressor
	stats []stat
}

type Service struct {
	clusterConnectTime atomic.Value
	connAddr           atomic.Value
	logger             logger.LogAdaptor
	mu                 *sync.RWMutex
	invFactory         *cluster.ConnectionInvocationFactory
	addrs              map[string]struct{}
	invocationService  *invocation.Service
	doneCh             chan struct{}
	ed                 *event.DispatchService
	btStats            binTextStats
	clientName         string
	gauges             []gauge
	interval           time.Duration
}

func NewService(
	invService *invocation.Service,
	invFactory *cluster.ConnectionInvocationFactory,
	ed *event.DispatchService,
	logger logger.LogAdaptor,
	interval time.Duration,
	clientName string) *Service {
	s := &Service{
		invocationService: invService,
		invFactory:        invFactory,
		doneCh:            make(chan struct{}),
		interval:          interval,
		logger:            logger,
		addrs:             map[string]struct{}{},
		mu:                &sync.RWMutex{},
		clientName:        clientName,
		ed:                ed,
		btStats:           binTextStats{mc: NewMetricCompressor()},
	}
	s.clusterConnectTime.Store(time.Now())
	s.connAddr.Store(pubcluster.NewAddress("", 0))
	ed.Subscribe(cluster.EventCluster, event.DefaultSubscriptionID, s.handleClusterEvent)
	s.addGauges()
	return s
}

func (s *Service) Start() {
	go s.loop()
}

func (s *Service) Stop() {
	close(s.doneCh)
	subID := event.MakeSubscriptionID(s.handleClusterEvent)
	s.ed.Unsubscribe(cluster.EventCluster, subID)
}

func (s *Service) loop() {
	timer := time.NewTimer(s.interval)
	defer timer.Stop()
	for {
		select {
		case <-s.doneCh:
			return
		case <-timer.C:
			ctx, cancel := context.WithTimeout(context.Background(), s.interval)
			s.sendStats(ctx)
			cancel()
			timer.Reset(s.interval)
		}
	}
}

func (s *Service) handleClusterEvent(event event.Event) {
	e := event.(*cluster.ClusterStateChangedEvent)
	if e.State == cluster.ClusterStateDisconnected {
		return
	}
	s.clusterConnectTime.Store(time.Now())
	s.connAddr.Store(e.Addr)
}

func (s *Service) sendStats(ctx context.Context) {
	s.mu.Lock()
	now := time.Now()
	s.addBasicStats(now)
	blob := s.makeStats()
	statsStr := makeStatString(s.btStats.stats)
	s.resetStats()
	s.mu.Unlock()
	s.logger.Debug(func() string {
		return fmt.Sprintf("sending stats: %s", statsStr)
	})
	request := codec.EncodeClientStatisticsRequest(now.Unix()*1000, statsStr, blob)
	inv := s.invFactory.NewInvocationOnRandomTarget(request, nil, time.Now())
	if err := s.invocationService.SendRequest(ctx, inv); err != nil {
		s.logger.Debug(func() string {
			return fmt.Sprintf("sending stats: %s", err.Error())
		})
	}
	if _, err := inv.GetWithContext(ctx); err != nil {
		s.logger.Debug(func() string {
			return fmt.Sprintf("sending stats: %s", err.Error())
		})
	}
}

func (s *Service) addBasicStats(ts time.Time) {
	lastTS := s.clusterConnectTime.Load().(time.Time)
	connAddr := s.connAddr.Load().(pubcluster.Address)
	s.btStats.stats = append(s.btStats.stats, MakeBasicStats(ts, lastTS, connAddr, s.clientName)...)
}

func (s *Service) makeStats() []byte {
	for _, gauge := range s.gauges {
		gauge.Update(&s.btStats)
	}
	blob := s.btStats.mc.GenerateBlob()
	return blob
}

func (s *Service) resetStats() {
	s.btStats.mc.Reset()
	s.btStats.stats = nil
}

func (s *Service) addGauges() {
	s.gauges = []gauge{
		newGaugeRuntime(s.logger),
		newGaugeOS(s.logger),
	}
}

func makeStatString(ss []stat) string {
	sb := strings.Builder{}
	if len(ss) == 0 {
		return ""
	}
	sb.WriteString(ss[0].k)
	sb.WriteString("=")
	sb.WriteString(ss[0].v)
	for _, s := range ss[1:] {
		sb.WriteString(",")
		sb.WriteString(s.k)
		sb.WriteString("=")
		sb.WriteString(s.v)
	}
	return sb.String()
}

func MakeBasicStats(lastTS time.Time, connTS time.Time, addr pubcluster.Address, clientName string) []stat {
	lastTSMS := int(lastTS.Unix() * 1000)
	connTSMS := int(connTS.Unix() * 1000)
	stats := []stat{
		{k: "lastStatisticsCollectionTime", v: strconv.Itoa(lastTSMS)},
		{k: "enterprise", v: "false"},
		{k: "clientType", v: internal.ClientType},
		{k: "clientVersion", v: internal.ClientVersion},
		{k: "clusterConnectionTimestamp", v: strconv.Itoa(connTSMS)},
		{k: "clientAddress", v: addr.String()},
		{k: "clientName", v: clientName},
	}
	return stats
}

type gauge interface {
	Update(btStats *binTextStats)
}

type runtimeGauges struct {
	logger          logger.LogAdaptor
	availProcessors metricDescriptor
	uptime          metricDescriptor
	totalMem        metricDescriptor
	usedMem         metricDescriptor
	freeMem         metricDescriptor
	maxHeap         metricDescriptor
	freeHeap        metricDescriptor
	usedHeap        metricDescriptor
	committedHeap   metricDescriptor
}

func newGaugeRuntime(lg logger.LogAdaptor) runtimeGauges {
	return runtimeGauges{
		logger:          lg,
		availProcessors: makeCountMD("runtime", "availableProcessors"),
		uptime:          makeMSMD("runtime", "uptime"),
		totalMem:        makeBytesMD("runtime", "totalMemory"),
		usedMem:         makeBytesMD("runtime", "usedMemory"),
		freeMem:         makeBytesMD("runtime", "freeMemory"),
		maxHeap:         makeBytesMD("memory", "maxHeap"),
		freeHeap:        makeBytesMD("memory", "freeHeap"),
		usedHeap:        makeBytesMD("memory", "usedHeap"),
		committedHeap:   makeBytesMD("memory", "committedHeap"),
	}
}

func (g runtimeGauges) Update(bt *binTextStats) {
	g.updateNumCPU(bt)
	g.updateUptime(bt)
	g.updateMem(bt)
}

func (g runtimeGauges) updateNumCPU(bt *binTextStats) {
	numCpu := int64(runtime.NumCPU())
	bt.mc.AddLong(g.availProcessors, numCpu)
	bt.stats = append(bt.stats, makeTextStat(&g.availProcessors, numCpu))
}

func (g runtimeGauges) updateUptime(bt *binTextStats) {
	if uptime, err := host.Uptime(); err != nil {
		// could not get the uptime
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting uptime: %s", err.Error())
		})
	} else {
		ms := int64(uptime) * 1000
		bt.mc.AddLong(g.uptime, ms)
		bt.stats = append(bt.stats, makeTextStat(&g.uptime, ms))
	}
}

func (g runtimeGauges) updateMem(bt *binTextStats) {
	ms := runtime.MemStats{}
	runtime.ReadMemStats(&ms)
	bt.mc.AddLong(g.totalMem, int64(ms.HeapSys))
	bt.mc.AddLong(g.usedMem, int64(ms.HeapInuse))
	bt.mc.AddLong(g.freeMem, int64(ms.HeapIdle))
	bt.mc.AddLong(g.maxHeap, int64(ms.HeapSys))
	bt.mc.AddLong(g.usedHeap, int64(ms.HeapInuse))
	bt.mc.AddLong(g.freeHeap, int64(ms.HeapIdle))
	bt.mc.AddLong(g.committedHeap, int64(ms.HeapAlloc))
	bt.stats = append(bt.stats,
		makeTextStat(&g.totalMem, ms.HeapSys),
		makeTextStat(&g.usedMem, ms.HeapInuse),
		makeTextStat(&g.freeMem, ms.HeapIdle),
		makeTextStat(&g.maxHeap, ms.HeapSys),
		makeTextStat(&g.usedHeap, ms.HeapInuse),
		makeTextStat(&g.freeHeap, ms.HeapIdle),
		makeTextStat(&g.committedHeap, ms.HeapAlloc))
}

type gaugeOS struct {
	logger        logger.LogAdaptor
	totalMem      metricDescriptor
	freeMem       metricDescriptor
	committedVM   metricDescriptor
	freeSwap      metricDescriptor
	totalSwap     metricDescriptor
	cpuTime       metricDescriptor
	loadAvg       metricDescriptor
	maxDecrCount  metricDescriptor
	openDecrCount metricDescriptor
}

func newGaugeOS(lg logger.LogAdaptor) gaugeOS {
	return gaugeOS{
		logger:        lg,
		totalMem:      makeBytesMD("os", "totalPhysicalMemorySize"),
		freeMem:       makeBytesMD("os", "freePhysicalMemorySize"),
		committedVM:   makeBytesMD("os", "committedVirtualMemorySize"),
		freeSwap:      makeBytesMD("os", "freeSwapSpaceSize"),
		totalSwap:     makeBytesMD("os", "totalSwapSpaceSize"),
		cpuTime:       makeBytesMD("os", "processCpuTime"),
		loadAvg:       makePercentMD("os", "systemLoadAverage"),
		maxDecrCount:  makeCountMD("os", "maxFileDescriptorCount"),
		openDecrCount: makeCountMD("os", "openFileDescriptorCount"),
	}
}

func (g gaugeOS) Update(bt *binTextStats) {
	g.updateVM(bt)
	g.updateSwap(bt)
	g.updateCPU(bt)
	g.updateLoad(bt)
	g.updateDescr(bt)
}

func (g gaugeOS) updateVM(bt *binTextStats) {
	if vs, err := mem.VirtualMemory(); err != nil {
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting virtual memory stats: %s", err.Error())
		})
	} else {
		bt.mc.AddLong(g.totalMem, int64(vs.Total))
		bt.mc.AddLong(g.freeMem, int64(vs.Free))
		bt.mc.AddLong(g.committedVM, int64(vs.CommittedAS))
		bt.stats = append(bt.stats,
			makeTextStat(&g.totalMem, vs.Total),
			makeTextStat(&g.freeMem, vs.Free),
			makeTextStat(&g.committedVM, vs.CommittedAS))
	}
}

func (g gaugeOS) updateSwap(bt *binTextStats) {
	if sm, err := mem.SwapMemory(); err != nil {
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting swap memory stats: %s", err.Error())
		})
	} else {
		bt.mc.AddLong(g.freeSwap, int64(sm.Free))
		bt.mc.AddLong(g.totalSwap, int64(sm.Total))
		bt.stats = append(bt.stats,
			makeTextStat(&g.freeSwap, sm.Free),
			makeTextStat(&g.totalSwap, sm.Total))
	}
}

func (g gaugeOS) updateCPU(bt *binTextStats) {
	if ts, err := cpu.Times(false); err != nil {
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting CPU stats: %s", err.Error())
		})
	} else if len(ts) == 0 {
		g.logger.Debug(func() string { return "ERROR getting CPU stats: no CPU found" })
	} else {
		cpuTime := ts[0].Total() * 1000
		bt.mc.AddLong(g.cpuTime, int64(cpuTime))
		bt.stats = append(bt.stats, makeTextStat(&g.cpuTime, cpuTime))
	}
}

func (g gaugeOS) updateLoad(bt *binTextStats) {
	if avg, err := load.Avg(); err != nil {
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting load average: %s", err.Error())
		})
	} else {
		bt.mc.AddDouble(g.loadAvg, avg.Load1)
		bt.stats = append(bt.stats, makeTextStat(&g.loadAvg, avg.Load1))
	}
}

func (g gaugeOS) updateDescr(bt *binTextStats) {
	if proc, err := process.NewProcess(int32(os.Getpid())); err != nil {
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting process info: %s", err.Error())
		})
	} else if rls, err := proc.Rlimit(); err != nil {
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting RLIMIT: %s", err.Error())
		})
	} else if nfd, err := proc.NumFDs(); err != nil {
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting open file descriptors: %s", err.Error())
		})
	} else {
		for _, rl := range rls {
			if rl.Resource == process.RLIMIT_NOFILE {
				bt.mc.AddLong(g.maxDecrCount, int64(rl.Soft))
				bt.stats = append(bt.stats, makeTextStat(&g.maxDecrCount, rl.Soft))
				break
			}
		}
		bt.mc.AddLong(g.openDecrCount, int64(nfd))
		bt.stats = append(bt.stats, makeTextStat(&g.openDecrCount, nfd))
	}
}

func makeBytesMD(prefix, metric string) metricDescriptor {
	return metricDescriptor{
		Prefix:  prefix,
		Metric:  metric,
		HasUnit: true,
		Unit:    metricUnitBytes,
	}
}

func makeMSMD(prefix, metric string) metricDescriptor {
	return metricDescriptor{
		Prefix:  prefix,
		Metric:  metric,
		HasUnit: true,
		Unit:    metricUnitMS,
	}
}

func makeCountMD(prefix, metric string) metricDescriptor {
	return metricDescriptor{
		Prefix:  prefix,
		Metric:  metric,
		HasUnit: true,
		Unit:    metricUnitCount,
	}
}

func makePercentMD(prefix, metric string) metricDescriptor {
	return metricDescriptor{
		Prefix:  prefix,
		Metric:  metric,
		HasUnit: true,
		Unit:    metricPercent,
	}
}

func makeTextStat(md *metricDescriptor, value interface{}) stat {
	return stat{k: md.String(), v: fmt.Sprintf("%v", value)}
}
