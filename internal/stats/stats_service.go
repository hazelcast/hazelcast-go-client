package stats

import (
	"context"
	"fmt"
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
	request := codec.EncodeClientStatisticsRequest(now.Unix()*1000, statsStr, []byte{})
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
