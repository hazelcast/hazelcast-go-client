package stats

import (
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

type Service struct {
	doneCh   chan struct{}
	interval time.Duration
	addr     atomic.Value
	logger   logger.Logger
}

func NewService(ed event.DispatchService, logger logger.Logger, interval time.Duration) *Service {
	s := &Service{
		doneCh:   make(chan struct{}),
		interval: interval,
		logger:   logger,
	}
	ed.Subscribe(cluster.EventConnectionOpened, event.DefaultSubscriptionID, s.handleConnectionOpened)
	return s
}

func (s Service) Stop() {
	close(s.doneCh)
}

func (s Service) loop() {
	for {
		select {
		case <-s.doneCh:
			return
		case <-time.After(s.interval):
		}
	}
}

func (s *Service) handleConnectionOpened(event event.Event) {
	if e, ok := event.(*cluster.ConnectionOpened); ok {
		s.addr.Store(e.Conn.Endpoint().String())
	}
}

func (s *Service) sendStats() {

}
