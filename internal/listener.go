package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"
)

type ListenerService struct {
	client        *HazelcastClient
	registrations map[string]int64
}

func newListenerService(client *HazelcastClient) *ListenerService {
	return &ListenerService{client: client, registrations: make(map[string]int64)}
}

func (listenerService *ListenerService) startListening(request *ClientMessage, eventHandler func(clientMessage *ClientMessage), responseDecoder DecodeListenerResponse, keyData *serialization.Data) (*string, error) {
	var invocation *Invocation
	if keyData != nil {
		partitionId := listenerService.client.PartitionService.GetPartitionId(keyData)
		invocation = NewInvocation(request, partitionId, nil, nil)
		invocation.eventHandler = eventHandler
	} else {
		invocation = NewInvocation(request, -1, nil, nil)
		invocation.eventHandler = eventHandler
	}
	responseMessage, err := listenerService.client.InvocationService.SendInvocation(invocation).Result()
	if err != nil {
		return nil, err
	}
	registrationId := responseDecoder(responseMessage)
	invocation.registrationId = registrationId
	listenerService.registrations[*registrationId] = request.CorrelationId()
	return registrationId, nil
}
