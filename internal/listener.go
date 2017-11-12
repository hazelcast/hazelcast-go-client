package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/internal/serialization"
	"sync/atomic"
)

type ListenerService struct {
	client                             *HazelcastClient
	registrations                      map[string]*Invocation
	registrationConnections            atomic.Value
	register                           chan *Invocation
	unregister                         chan *string
	unregisterResult                   chan int64
}
type registrationInvocation struct {
	registrationId string
	invocation *Invocation
}

func newListenerService(client *HazelcastClient) *ListenerService {
	service := &ListenerService{client: client, registrations: make(map[string]*Invocation), register: make(chan *Invocation, 1), unregister: make(chan *string, 1),
		unregisterResult:                   make(chan int64, 0),
	}
	service.registrationConnections.Store(make(map[string]map[*Connection]struct{}))
	go service.process()
	return service
}
func (listenerService *ListenerService) process() {
	for {
		select {
		case invocation := <-listenerService.register:
			listenerService.registrations[*invocation.registrationId] = invocation
		case registrationId := <-listenerService.unregister:
			invocation, found := listenerService.registrations[*registrationId]
			correlationId := invocation.request.CorrelationId()
			if found {
				delete(listenerService.registrations, *registrationId)
				listenerService.unregisterResult <- correlationId
			} else {
				listenerService.unregisterResult <- -1
			}
		}

	}
}
func (listenerService *ListenerService) startListening(request *ClientMessage, eventHandler func(clientMessage *ClientMessage),
	responseDecoder DecodeListenerResponse, keyData *serialization.Data) (*string, error) {
	var invocation *Invocation
	if keyData != nil {
		partitionId := listenerService.client.PartitionService.GetPartitionId(keyData)
		invocation = NewInvocation(request, partitionId, nil, nil)
	} else {
		invocation = NewInvocation(request, -1, nil, nil)
	}
	invocation.eventHandler = eventHandler
	invocation.listenerResponseDecoder = responseDecoder
	connections := listenerService.client.ConnectionManager.getActiveConnections()
	var responseMessage *ClientMessage
	var registrationId *string
	var err error
	registrationConnections := listenerService.registrationConnections.Load().(map[string]map[*Connection]struct{})
	copyregistrationConnections := make(map[string]map[*Connection]struct{})
	//Deep copy
	for k, v := range registrationConnections {
		copyregistrationConnections[k] = make(map[*Connection]struct{})
		for k2, v2 := range v {
			copyregistrationConnections[k][k2] = v2
		}
	}
	for _, connection := range connections {
		invocation.boundConnection = connection
		responseMessage, err = listenerService.client.InvocationService.sendInvocation(invocation).Result()
		if err != nil {
			if connection.IsAlive() {
				//TODO:: remove listener from members
			}
			return nil, err
		} else {
			registrationId = responseDecoder(responseMessage)
			copyregistrationConnections[*registrationId] = make(map[*Connection]struct{})
			copyregistrationConnections[*registrationId][connection] = struct{}{}
		}
	}
	listenerService.registrationConnections.Store(copyregistrationConnections)
	invocation.registrationId = registrationId
	listenerService.register <- invocation
	return registrationId, nil
}
func (listenerService *ListenerService) stopListening(registrationId *string, requestEncoder EncodeListenerRemoveRequest) (bool, error) {
	listenerService.unregister <- registrationId
	correlationId := <-listenerService.unregisterResult
	if correlationId == -1 {
		return false, nil
	}
	listenerService.client.InvocationService.removeEventHandler(correlationId)
	registrationConnections := listenerService.registrationConnections.Load().(map[string]map[*Connection]struct{})
	if _, found := registrationConnections[*registrationId]; !found {
		return false, nil
	}
	copyregistrationConnections := make(map[string]map[*Connection]struct{})
	//Deep copy
	for k, v := range registrationConnections {
		copyregistrationConnections[k] = make(map[*Connection]struct{})
		for k2, v2 := range v {
			copyregistrationConnections[k][k2] = v2
		}
	}
	for k, v := range registrationConnections {
		copyregistrationConnections[k] = v
	}
	for connection, _ := range copyregistrationConnections[*registrationId] {
		_, err := listenerService.client.InvocationService.InvokeOnConnection(requestEncoder(registrationId), connection).Result()
		if err != nil {
			return false, err
		} else {
			delete(copyregistrationConnections[*registrationId], connection)
		}
	}
	listenerService.registrationConnections.Store(copyregistrationConnections)
	return true, nil
}

func (listenerService *ListenerService) reregisterListener(invocation *Invocation) {
	newInvocation := NewInvocation(invocation.request, invocation.partitionId, nil, nil)
	newInvocation.eventHandler = invocation.eventHandler
	newInvocation.listenerResponseDecoder = invocation.listenerResponseDecoder
	responseMessage, err := listenerService.client.InvocationService.sendInvocation(invocation).Result()
	if err != nil {
		return
	}
	//TODO:: Should we remove the listener with the previous registration id
	registrationId := newInvocation.listenerResponseDecoder(responseMessage)
	newInvocation.registrationId = registrationId
	listenerService.register <- newInvocation
}
func (listenerService *ListenerService) onConnectionClosed(connection *Connection) {

}
func (listenerService *ListenerService) onConnectionOpened(connection *Connection) {
	for _,invocation := range listenerService.registrations{
		invocation.boundConnection=connection
		_,err:=listenerService.client.InvocationService.sendInvocation(invocation).Result()
		if err== nil{
			//TODO add connection to registraionId's connections

		}

	}
}
