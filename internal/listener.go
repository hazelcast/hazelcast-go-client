package internal

import (
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"
)

type ListenerService struct {
	client           *HazelcastClient
	registrations    map[string]int64
	register         chan *Invocation
	unregister       chan *string
	unregisterResult chan int64
}

func newListenerService(client *HazelcastClient) *ListenerService {
	service := &ListenerService{client: client, registrations: make(map[string]int64), register: make(chan *Invocation, 1), unregister: make(chan *string, 1),
		unregisterResult: make(chan int64, 0),
	}
	go service.process()
	return service
}
func (listenerService *ListenerService) process() {
	for {
		select {
		case invocation := <-listenerService.register:
			listenerService.registrations[*invocation.registrationId] = invocation.request.CorrelationId()
		case registrationId := <-listenerService.unregister:
			correlationId, found := listenerService.registrations[*registrationId]
			if found {
				delete(listenerService.registrations, *registrationId)
				listenerService.unregisterResult <- correlationId
			} else {
				listenerService.unregisterResult <- -1
			}
		}
	}
}
func (listenerService *ListenerService) startListening(request *ClientMessage, eventHandler func(clientMessage *ClientMessage), responseDecoder DecodeListenerResponse, keyData *serialization.Data) (*string, error) {
	var invocation *Invocation
	if keyData != nil {
		partitionId := listenerService.client.PartitionService.GetPartitionId(keyData)
		invocation = NewInvocation(request, partitionId, nil, nil)
	} else {
		invocation = NewInvocation(request, -1, nil, nil)
	}
	invocation.eventHandler = eventHandler
	invocation.listenerResponseDecoder = responseDecoder
	responseMessage, err := listenerService.client.InvocationService.SendInvocation(invocation).Result()
	if err != nil {
		return nil, err
	}
	registrationId := responseDecoder(responseMessage)
	invocation.registrationId = registrationId
	listenerService.register <- invocation
	return registrationId, nil
}
func (listenerService *ListenerService) stopListening(registrationId *string, requestEncoder EncodeListenerRemoveRequest) error {
	listenerService.unregister <- registrationId
	correlationId := <-listenerService.unregisterResult
	if correlationId == -1 {
		return common.NewHazelcastKeyError("Couldn't find the listener for the given registrationId", nil)
	}
	err := listenerService.client.InvocationService.removeEventHandler(correlationId)
	if err != nil {
		return err
	}

	_, err = listenerService.client.InvocationService.InvokeOnRandomTarget(requestEncoder(registrationId)).Result()
	return err
}

func (listenerService *ListenerService) reregisterListener(invocation *Invocation) {
	newInvocation := NewInvocation(invocation.request, invocation.partitionId, nil, nil)
	newInvocation.eventHandler = invocation.eventHandler
	newInvocation.listenerResponseDecoder = invocation.listenerResponseDecoder
	responseMessage, err := listenerService.client.InvocationService.SendInvocation(invocation).Result()
	if err != nil {
		return
	}
	//TODO:: Should we remove the listener with the previous registration id
	registrationId := newInvocation.listenerResponseDecoder(responseMessage)
	newInvocation.registrationId = registrationId
	listenerService.register <- newInvocation
}
