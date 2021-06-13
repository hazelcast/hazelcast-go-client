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

type Service struct {
	clusterConnectTime atomic.Value
	connAddr           atomic.Value
	logger             logger.Logger
	mu                 *sync.RWMutex
	invFactory         *cluster.ConnectionInvocationFactory
	addrs              map[string]struct{}
	requestCh          chan<- invocation.Invocation
	doneCh             chan struct{}
	ed                 *event.DispatchService
	mc                 *MetricsCompressor
	clientName         string
	gauges             []gauge
	interval           time.Duration
}

func NewService(
	requestCh chan<- invocation.Invocation,
	invFactory *cluster.ConnectionInvocationFactory,
	ed *event.DispatchService,
	logger logger.Logger,
	interval time.Duration,
	clientName string) *Service {
	s := &Service{
		requestCh:  requestCh,
		invFactory: invFactory,
		doneCh:     make(chan struct{}),
		interval:   interval,
		logger:     logger,
		addrs:      map[string]struct{}{},
		mu:         &sync.RWMutex{},
		clientName: clientName,
		ed:         ed,
		mc:         NewMetricCompressor(),
	}
	s.clusterConnectTime.Store(time.Now())
	s.connAddr.Store(pubcluster.NewAddress("", 0))
	ed.Subscribe(cluster.EventConnected, event.DefaultSubscriptionID, s.handleClusterConnected)
	s.addGauges()
	return s
}

func (s *Service) Start() {
	go s.loop()
}

func (s Service) Stop() {
	close(s.doneCh)
	subID := event.MakeSubscriptionID(s.handleClusterConnected)
	s.ed.Unsubscribe(cluster.EventConnected, subID)
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

func (s *Service) handleClusterConnected(event event.Event) {
	if e, ok := event.(*cluster.Connected); ok {
		s.clusterConnectTime.Store(time.Now())
		s.connAddr.Store(e.Addr)
	}
}

func (s *Service) sendStats(ctx context.Context) {
	now := time.Now()
	statsStr := makeStatString(s.basicStats(now))
	s.logger.Debug(func() string {
		return fmt.Sprintf("sending stats: %s", statsStr)
	})
	blob := s.makeStatBlob()
	request := codec.EncodeClientStatisticsRequest(now.Unix()*1000, statsStr, blob)
	inv := s.invFactory.NewInvocationOnRandomTarget(request, nil)
	select {
	case <-ctx.Done():
		break
	case s.requestCh <- inv:
		if _, err := inv.GetWithContext(ctx); err == nil {
			return
		}
	}
	s.logger.Debug(func() string { return fmt.Sprintf("error sending stats: %s", ctx.Err().Error()) })
}

func (s *Service) basicStats(ts time.Time) []stat {
	lastTS := s.clusterConnectTime.Load().(time.Time)
	connAddr := s.connAddr.Load().(*pubcluster.AddressImpl).String()
	return MakeBasicStats(ts, lastTS, connAddr, s.clientName)
}

func (s *Service) makeStatBlob() []byte {
	for _, gauge := range s.gauges {
		gauge.Update(s.mc)
	}
	blob := s.mc.GenerateBlob()
	s.mc.Reset()
	return blob
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

func MakeBasicStats(lastTS time.Time, connTS time.Time, addr, clientName string) []stat {
	lastTSMS := int(lastTS.Unix() * 1000)
	connTSMS := int(connTS.Unix() * 1000)
	stats := []stat{
		{k: "lastStatisticsCollectionTime", v: strconv.Itoa(lastTSMS)},
		{k: "enterprise", v: "false"},
		{k: "clientType", v: internal.ClientType},
		{k: "clientVersion", v: internal.ClientVersion},
		{k: "clusterConnectionTimestamp", v: strconv.Itoa(connTSMS)},
		{k: "clientAddress", v: addr},
		{k: "clientName", v: clientName},
	}
	return stats
}

type gauge interface {
	Update(mc *MetricsCompressor)
}

type runtimeGauges struct {
	logger          logger.Logger
	availProcessors metricDescriptor
	uptime          metricDescriptor
	totalMem        metricDescriptor
	usedMem         metricDescriptor
	freeMem         metricDescriptor
}

func newGaugeRuntime(lg logger.Logger) runtimeGauges {
	return runtimeGauges{
		logger:          lg,
		availProcessors: makeCountMD("runtime", "availableProcessors"),
		uptime:          makeMSMD("runtime", "uptime"),
		totalMem:        makeBytesMD("runtime", "totalMemory"),
		usedMem:         makeBytesMD("runtime", "usedMemory"),
		freeMem:         makeBytesMD("runtime", "freeMemory"),
	}
}

func (g runtimeGauges) Update(mc *MetricsCompressor) {
	mc.AddLong(g.availProcessors, int64(runtime.NumCPU()))
	g.updateUptime(mc)
	g.updateMem(mc)
}

func (g runtimeGauges) updateUptime(mc *MetricsCompressor) {
	if uptime, err := host.Uptime(); err != nil {
		// could not get the uptime
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting uptime: %s", err.Error())
		})
	} else {
		mc.AddLong(g.uptime, int64(uptime)*1000)
	}
}

func (g runtimeGauges) updateMem(mc *MetricsCompressor) {
	ms := runtime.MemStats{}
	runtime.ReadMemStats(&ms)
	mc.AddLong(g.totalMem, int64(ms.HeapSys))
	mc.AddLong(g.usedMem, int64(ms.HeapInuse))
	mc.AddLong(g.freeMem, int64(ms.HeapIdle))
}

type gaugeOS struct {
	logger        logger.Logger
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

func newGaugeOS(lg logger.Logger) gaugeOS {
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

func (g gaugeOS) Update(mc *MetricsCompressor) {
	g.updateVM(mc)
	g.updateSwap(mc)
	g.updateCPU(mc)
	g.updateLoad(mc)
	g.updateDescr(mc)
}

func (g gaugeOS) updateVM(mc *MetricsCompressor) {
	if vs, err := mem.VirtualMemory(); err != nil {
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting virtual memory stats: %s", err.Error())
		})
	} else {
		mc.AddLong(g.totalMem, int64(vs.Total))
		mc.AddLong(g.freeMem, int64(vs.Free))
		mc.AddLong(g.committedVM, int64(vs.CommittedAS))
	}
}

func (g gaugeOS) updateSwap(mc *MetricsCompressor) {
	if sm, err := mem.SwapMemory(); err != nil {
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting swap memory stats: %s", err.Error())
		})
	} else {
		mc.AddLong(g.freeSwap, int64(sm.Free))
		mc.AddLong(g.totalSwap, int64(sm.Total))
	}
}

func (g gaugeOS) updateCPU(mc *MetricsCompressor) {
	if ts, err := cpu.Times(false); err != nil {
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting CPU stats: %s", err.Error())
		})
	} else if len(ts) == 0 {
		g.logger.Debug(func() string { return "ERROR getting CPU stats: no CPU found" })
	} else {
		mc.AddLong(g.cpuTime, int64(ts[0].Total()*1000))
	}
}

func (g gaugeOS) updateLoad(mc *MetricsCompressor) {
	if avg, err := load.Avg(); err != nil {
		g.logger.Debug(func() string {
			return fmt.Sprintf("ERROR getting load average: %s", err.Error())
		})
	} else {
		mc.AddDouble(g.loadAvg, avg.Load1)
	}
}

func (g gaugeOS) updateDescr(mc *MetricsCompressor) {
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
				mc.AddLong(g.maxDecrCount, int64(rl.Soft))
				break
			}
		}
		mc.AddLong(g.openDecrCount, int64(nfd))
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
