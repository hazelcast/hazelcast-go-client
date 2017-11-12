package internal

import (
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"log"
)

type ListenerService struct {
	client                                     *HazelcastClient
	registrations                              map[string]map[*Connection]*eventRegistration
	registrationIdToListenerRegistration       map[string]*listenerRegistrationKey
	failedRegistrations                        map[*Connection][]*listenerRegistrationKey
	register                                   chan *Invocation
	registerListenerOnConnectionChannel        chan registrationIdConnection
	registerListenerOnConnectionErrChannel     chan error
	deregisterListenerChannel                  chan registrationIdRequestEncoder
	deregisterListenerErrChannel               chan removedErr
	onConnectionClosedChannel                  chan *Connection
	onConnectionOpenedChannel                  chan *Connection
	OnHeartbeatRestoredChannel                 chan *Connection
	registerListenerInternalHandleErrorChannel chan registrationIdConnection
	registerListenerInitChannel                chan *listenerRegistrationKey
}
type removedErr struct {
	removed bool
	err     error
}
type registrationIdRequestEncoder struct {
	registrationId string
	requestEncoder EncodeListenerRemoveRequest
}
type registrationIdConnection struct {
	registrationId string
	connection     *Connection
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

		registrations:                              make(map[string]map[*Connection]*eventRegistration),
		registrationIdToListenerRegistration:       make(map[string]*listenerRegistrationKey),
		failedRegistrations:                        make(map[*Connection][]*listenerRegistrationKey),
		registerListenerOnConnectionChannel:        make(chan registrationIdConnection, 1),
		registerListenerOnConnectionErrChannel:     make(chan error, 1),
		deregisterListenerChannel:                  make(chan registrationIdRequestEncoder, 1),
		deregisterListenerErrChannel:               make(chan removedErr, 1),
		onConnectionClosedChannel:                  make(chan *Connection, 1),
		onConnectionOpenedChannel:                  make(chan *Connection, 1),
		OnHeartbeatRestoredChannel:                 make(chan *Connection, 1),
		registerListenerInternalHandleErrorChannel: make(chan registrationIdConnection, 1),
		registerListenerInitChannel:                make(chan *listenerRegistrationKey, 1),
	}
	service.client.ConnectionManager.AddListener(service)
	service.client.HeartBeatService.AddHeartbeatListener(service)
	go service.process()

	return service
}

func (listenerService *ListenerService) process() {
	for {
		select {
		case registrationIdConnection := <-listenerService.registerListenerOnConnectionChannel:
			listenerService.registerListenerOnConnectionErrChannel <- listenerService.registerListenerOnConnection(registrationIdConnection.registrationId, registrationIdConnection.connection)
		case registrationIdRequestEncoder := <-listenerService.deregisterListenerChannel:
			removed, err := listenerService.deregisterListenerInternal(registrationIdRequestEncoder.registrationId, registrationIdRequestEncoder.requestEncoder)
			removedErr := removedErr{
				removed: removed,
				err:     err,
			}
			listenerService.deregisterListenerErrChannel <- removedErr
		case connection := <-listenerService.onConnectionClosedChannel:
			listenerService.onConnectionClosedInternal(connection)
		case connection := <-listenerService.onConnectionOpenedChannel:
			listenerService.onConnectionOpenedInternal(connection)
		case connection := <-listenerService.OnHeartbeatRestoredChannel:
			listenerService.OnHeartbeatRestoredInternal(connection)
		case registrationIdConnection := <-listenerService.registerListenerInternalHandleErrorChannel:
			listenerService.registerListenerFromInternalHandleError(registrationIdConnection.registrationId, registrationIdConnection.connection)
		case listenerRegistrationKey := <-listenerService.registerListenerInitChannel:
			listenerService.registerListenerInit(listenerRegistrationKey)
		}
	}

}
func (listenerService *ListenerService) registerListenerInit(key *listenerRegistrationKey) {
	listenerService.registrationIdToListenerRegistration[key.userRegistrationKey] = key
	listenerService.registrations[key.userRegistrationKey] = make(map[*Connection]*eventRegistration)
}
func (listenerService *ListenerService) registerListener(request *ClientMessage, eventHandler func(clientMessage *ClientMessage),
	encodeListenerRemoveRequest EncodeListenerRemoveRequest, responseDecoder DecodeListenerResponse) (*string, error) {
	userRegistrationId, _ := common.NewUUID()
	registrationKey := listenerRegistrationKey{
		userRegistrationKey: userRegistrationId,
		request:             request,
		responseDecoder:     responseDecoder,
		eventHandler:        eventHandler,
	}
	connections := listenerService.client.ConnectionManager.getActiveConnections()
	listenerService.registerListenerInitChannel <- &registrationKey
	for _, connection := range connections {
		registrationIdConnection := registrationIdConnection{
			registrationId: userRegistrationId,
			connection:     connection,
		}
		listenerService.registerListenerOnConnectionChannel <- registrationIdConnection
		err := <-listenerService.registerListenerOnConnectionErrChannel
		if err != nil {
			if connection.IsAlive() {
				listenerService.deregisterListener(userRegistrationId, encodeListenerRemoveRequest)
				return nil, common.NewHazelcastErrorType("listener cannot be added", nil)
			}
		}
	}

	return &userRegistrationId, nil
}
func (listenerService *ListenerService) registerListenerOnConnection(registrationId string, connection *Connection) error {
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
	return nil
}
func (listenerService *ListenerService) deregisterListener(registrationId string, requestEncoder EncodeListenerRemoveRequest) (bool, error) {
	registrationIdRequestEncoder := registrationIdRequestEncoder{
		registrationId: registrationId,
		requestEncoder: requestEncoder,
	}
	listenerService.deregisterListenerChannel <- registrationIdRequestEncoder
	removedErr := <-listenerService.deregisterListenerErrChannel
	return removedErr.removed, removedErr.err
}
func (listenerService *ListenerService) deregisterListenerInternal(registrationId string, requestEncoder EncodeListenerRemoveRequest) (bool, error) {
	var registrationMap map[*Connection]*eventRegistration
	var found bool
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
	return successful, err
}
func (listenerService *ListenerService) registerListenerFromInternal(registrationId string, connection *Connection) {
	registrationIdConnection := registrationIdConnection{
		registrationId: registrationId,
		connection:     connection,
	}
	listenerService.registerListenerOnConnectionChannel <- registrationIdConnection
	err := <-listenerService.registerListenerOnConnectionErrChannel
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
func (listenerService *ListenerService) registerListenerFromInternalHandleError(registrationId string, connection *Connection) {
	failedRegsToConnection, found := listenerService.failedRegistrations[connection]
	if !found {
		listenerService.failedRegistrations[connection] = make([]*listenerRegistrationKey, 0)
	}
	registrationKey := listenerService.registrationIdToListenerRegistration[registrationId]
	listenerService.failedRegistrations[connection] = append(failedRegsToConnection, registrationKey)
}
func (listenerService *ListenerService) onConnectionClosed(connection *Connection, cause error) {
	listenerService.onConnectionClosedChannel <- connection
}
func (listenerService *ListenerService) onConnectionClosedInternal(connection *Connection) {
	delete(listenerService.failedRegistrations, connection)
	for _, registrationMap := range listenerService.registrations {
		registration, found := registrationMap[connection]
		if found {
			delete(registrationMap, connection)
			listenerService.client.InvocationService.removeEventHandler(registration.correlationId)
		}
	}
}
func (listenerService *ListenerService) onConnectionOpened(connection *Connection) {
	listenerService.onConnectionOpenedChannel <- connection
}
func (listenerService *ListenerService) onConnectionOpenedInternal(connection *Connection) {
	for registrationKey, _ := range listenerService.registrations {
		go listenerService.registerListenerFromInternal(registrationKey, connection)
	}
}
func (listenerService *ListenerService) OnHeartbeatRestored(connection *Connection) {
	listenerService.OnHeartbeatRestoredChannel <- connection
}
func (listenerService *ListenerService) OnHeartbeatRestoredInternal(connection *Connection) {
	registrationKeys := listenerService.failedRegistrations[connection]
	for _, registrationKey := range registrationKeys {
		go listenerService.registerListenerFromInternal(registrationKey.userRegistrationKey, connection)
	}
}
