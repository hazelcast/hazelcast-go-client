package internal

import (
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"log"
	"sync"
)

type ListenerService struct {
	client                               *HazelcastClient
	registrations                        map[string]map[*Connection]*eventRegistration
	registrationIdToListenerRegistration map[string]*listenerRegistrationKey
	failedRegistrations                  map[*Connection][]*listenerRegistrationKey
	register                             chan *Invocation
	unregister                           chan *string
	unregisterResult                     chan int64
	registrationsChannel                 chan *sync.WaitGroup
	failedRegistrationsChannel           chan *sync.WaitGroup
}
type eventRegistration struct {
	serverRegistrationId string
	correlationId        int64
	connection           *Connection
}
type listenerRegistrationKey struct {
	userRegistrationKey string
	request             *ClientMessage
	responseDecoder     DecodeListenerResponse
	eventHandler        func(clientMessage *ClientMessage)
}

func newListenerService(client *HazelcastClient) *ListenerService {
	service := &ListenerService{client: client, register: make(chan *Invocation, 1),
		unregister:                           make(chan *string, 1),
		unregisterResult:                     make(chan int64, 0),
		registrations:                        make(map[string]map[*Connection]*eventRegistration),
		registrationIdToListenerRegistration: make(map[string]*listenerRegistrationKey),
		failedRegistrations:                  make(map[*Connection][]*listenerRegistrationKey),
		registrationsChannel:                 make(chan *sync.WaitGroup, 1),
		failedRegistrationsChannel:           make(chan *sync.WaitGroup, 1),
	}
	service.client.ConnectionManager.AddListener(service)
	service.client.HeartBeatService.AddHeartbeatListener(service)
	go service.process()

	return service
}
func (listenerService *ListenerService) process() {
	go func() {
		for {
			select {
			case wg := <-listenerService.failedRegistrationsChannel:
				wg.Wait()
			}
		}
	}()
	go func() {
		for {
			select {
			case wg := <-listenerService.registrationsChannel:
				wg.Wait()
			}
		}
	}()
}
func (listenerService *ListenerService) startListening(request *ClientMessage, eventHandler func(clientMessage *ClientMessage),
	encodeListenerRemoveRequest EncodeListenerRemoveRequest, responseDecoder DecodeListenerResponse) (*string, error) {
	userRegistrationId, _ := common.NewUUID()
	registrationKey := listenerRegistrationKey{
		userRegistrationKey: userRegistrationId,
		request:             request,
		responseDecoder:     responseDecoder,
		eventHandler:        eventHandler,
	}
	connections := listenerService.client.ConnectionManager.getActiveConnections()
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	listenerService.registrationsChannel <- wg
	wg.Add(1)
	listenerService.registrationIdToListenerRegistration[userRegistrationId] = &registrationKey
	listenerService.registrations[userRegistrationId] = make(map[*Connection]*eventRegistration)
	wg.Done()
	for _, connection := range connections {
		err := listenerService.registerListenerOnConnection(userRegistrationId, connection)
		if err != nil {
			if connection.IsAlive() {
				listenerService.stopListening(userRegistrationId, encodeListenerRemoveRequest)
				return nil, common.NewHazelcastErrorType("listener cannot be added", nil)
			}
		}
	}

	return &userRegistrationId, nil
}
func (listenerService *ListenerService) registerListenerOnConnection(registrationId string, connection *Connection) error {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	listenerService.registrationsChannel <- wg
	wg.Add(1)
	if registrationMap, found := listenerService.registrations[registrationId]; found {
		_, found := registrationMap[connection]
		if found {
			return nil
		}
	}
	registrationKey := listenerService.registrationIdToListenerRegistration[registrationId]
	invocation := NewInvocation(registrationKey.request, -1, nil, connection)
	invocation.eventHandler = registrationKey.eventHandler
	invocation.listenerResponseDecoder = registrationKey.responseDecoder
	responseMessage, err := listenerService.client.InvocationService.sendInvocation(invocation).Result()
	if err != nil {
		return err
	}
	serverRegistrationId := *registrationKey.responseDecoder(responseMessage)
	correlationId := registrationKey.request.CorrelationId()
	registration := &eventRegistration{
		serverRegistrationId: serverRegistrationId,
		correlationId:        correlationId,
		connection:           connection,
	}
	listenerService.registrations[registrationId][connection] = registration
	wg.Done()
	return nil
}
func (listenerService *ListenerService) stopListening(registrationId string, requestEncoder EncodeListenerRemoveRequest) (bool, error) {
	var registrationMap map[*Connection]*eventRegistration
	var found bool
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	listenerService.registrationsChannel <- wg
	wg.Add(1)
	if registrationMap, found = listenerService.registrations[registrationId]; !found {
		return false, nil
	}
	var successful bool = true
	var err error
	for _, registration := range registrationMap {
		connection := registration.connection
		serverRegistrationId := registration.serverRegistrationId
		request := requestEncoder(&serverRegistrationId)
		invocation := NewInvocation(request, -1, nil, connection)
		_, err = listenerService.client.InvocationService.sendInvocation(invocation).Result()
		if err != nil {
			if connection.IsAlive() {
				successful = false
				log.Println("deregistration of listener with ID ", registrationId, " has failed to address ", connection.endpoint.Host(),
					":", connection.endpoint.Port())
				continue
			}
		}
		listenerService.client.InvocationService.removeEventHandler(registration.correlationId)
		delete(registrationMap, connection)
	}
	if successful {
		delete(listenerService.registrations, registrationId)
		delete(listenerService.registrationIdToListenerRegistration, registrationId)
	}
	wg.Done()
	return successful, err
}
func (listenerService *ListenerService) registerListenerFromInternal(registrationId string, connection *Connection) {
	err := listenerService.registerListenerOnConnection(registrationId, connection)
	if err != nil {
		if _, ok := err.(*common.HazelcastIOError); ok {
			failedRegsToConnection, found := listenerService.failedRegistrations[connection]
			if !found {
				listenerService.failedRegistrations[connection] = make([]*listenerRegistrationKey, 0)
			}
			registrationKey := listenerService.registrationIdToListenerRegistration[registrationId]
			listenerService.failedRegistrations[connection] = append(failedRegsToConnection, registrationKey)
		} else {
			log.Println("listener ", registrationId, " cannot be added to a new connection ", connection, ", reason :", err)
		}
	}
}
func (listenerService *ListenerService) onConnectionClosed(connection *Connection, cause error) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	listenerService.failedRegistrationsChannel <- wg
	wg.Add(1)
	delete(listenerService.failedRegistrations, connection)
	wg.Done()
	listenerService.registrationsChannel <- wg
	wg.Add(1)
	for _, registrationMap := range listenerService.registrations {
		registration, found := registrationMap[connection]
		if found {
			delete(registrationMap, connection)
			listenerService.client.InvocationService.removeEventHandler(registration.correlationId)
		}
	}
	wg.Done()
}
func (listenerService *ListenerService) onConnectionOpened(connection *Connection) {
	for registrationKey, _ := range listenerService.registrations {
		listenerService.registerListenerFromInternal(registrationKey, connection)
	}
}
func (listenerService *ListenerService) OnHeartbeatRestored(connection *Connection) {
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	listenerService.failedRegistrationsChannel <- wg
	wg.Add(1)
	registrationKeys := listenerService.failedRegistrations[connection]
	wg.Done()
	for _, registrationKey := range registrationKeys {
		listenerService.registerListenerFromInternal(registrationKey.userRegistrationKey, connection)
	}
}
