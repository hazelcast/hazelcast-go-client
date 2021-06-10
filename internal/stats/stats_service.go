package stats

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	requestCh          chan<- invocation.Invocation
	invFactory         *cluster.ConnectionInvocationFactory
	addrs              map[string]struct{}
	mu                 *sync.RWMutex
	doneCh             chan struct{}
	ed                 *event.DispatchService
	clientName         string
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
	}
	s.clusterConnectTime.Store(time.Now())
	s.connAddr.Store(pubcluster.NewAddress("", 0))
	ed.Subscribe(cluster.EventConnected, event.DefaultSubscriptionID, s.handleClusterConnected)
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
			s.sendStats()
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

func (s *Service) sendStats() {
	now := time.Now()
	statsStr := makeStatString(append(s.basicStats(now), s.systemStats()...))
	s.logger.Debug(func() string {
		return fmt.Sprintf("sending stats: %s", statsStr)
	})
	request := codec.EncodeClientStatisticsRequest(now.Unix()*1000, statsStr, []byte{})
	inv := s.invFactory.NewInvocationOnRandomTarget(request, nil)
	s.requestCh <- inv
}

func (s *Service) basicStats(ts time.Time) []stat {
	lastTs := int(ts.Unix() * 1000)
	connTs := int(s.clusterConnectTime.Load().(time.Time).Unix() * 1000)
	stats := []stat{
		{k: "lastStatisticsCollectionTime", v: strconv.Itoa(lastTs)},
		{k: "enterprise", v: "false"},
		{k: "clientType", v: internal.ClientType},
		{k: "clientVersion", v: internal.ClientVersion},
		{k: "clusterConnectionTimestamp", v: strconv.Itoa(connTs)},
		{k: "clientAddress", v: s.connAddr.Load().(*pubcluster.AddressImpl).String()},
		{k: "clientName", v: s.clientName},
	}
	return stats
}

func (s *Service) systemStats() []stat {
	stats := []stat{
		{k: "runtime.availableProcessors", v: strconv.Itoa(runtime.NumCPU())},
	}
	return stats
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
