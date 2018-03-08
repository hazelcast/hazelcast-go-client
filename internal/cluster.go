// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/common"
	. "github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DEFAULT_ADDRESS       = "127.0.0.1"
	DEFAULT_PORT          = 5701
	MEMBER_ADDED    int32 = 1
	MEMBER_REMOVED  int32 = 2
)

var wg sync.WaitGroup

type ClusterService struct {
	client                 *HazelcastClient
	config                 *config.ClientConfig
	members                atomic.Value
	ownerUuid              atomic.Value
	uuid                   atomic.Value
	ownerConnectionAddress atomic.Value
	listeners              atomic.Value
	mu                     sync.Mutex
	reconnectChan          chan struct{}
}

func NewClusterService(client *HazelcastClient, config *config.ClientConfig) *ClusterService {
	service := &ClusterService{client: client, config: config, reconnectChan: make(chan struct{}, 1)}
	service.ownerConnectionAddress.Store(&Address{})
	service.members.Store(make([]*Member, 0))             //Initialize
	service.listeners.Store(make(map[string]interface{})) //Initialize
	service.ownerUuid.Store("")                           //Initialize
	service.uuid.Store("")                                //Initialize
	for _, membershipListener := range client.ClientConfig.MembershipListeners() {
		service.AddListener(membershipListener)
	}
	service.client.ConnectionManager.AddListener(service)
	go service.process()
	return service
}
func (clusterService *ClusterService) start() error {
	return clusterService.connectToCluster()
}
func getPossibleAddresses(addressList []string, memberList []*Member) []Address {
	if addressList == nil {
		addressList = make([]string, 0)
	}
	if memberList == nil {
		memberList = make([]*Member, 0)
	}
	allAddresses := make(map[Address]struct{}, len(addressList)+len(memberList))
	for _, address := range addressList {
		ip, port := common.GetIpAndPort(address)
		if common.IsValidIpAddress(ip) {
			if port == -1 {
				allAddresses[*NewAddressWithParameters(ip, DEFAULT_PORT)] = struct{}{}
				allAddresses[*NewAddressWithParameters(ip, DEFAULT_PORT+1)] = struct{}{}
				allAddresses[*NewAddressWithParameters(ip, DEFAULT_PORT+2)] = struct{}{}
			} else {
				allAddresses[*NewAddressWithParameters(ip, port)] = struct{}{}
			}

		}
	}
	for _, member := range memberList {
		allAddresses[*member.Address().(*Address)] = struct{}{}
	}
	addresses := make([]Address, len(allAddresses))
	index := 0
	for k, _ := range allAddresses {
		addresses[index] = k
		index++
	}
	if len(addresses) == 0 {
		addresses = append(addresses, *NewAddressWithParameters(DEFAULT_ADDRESS, DEFAULT_PORT))
	}
	return addresses
}
func (clusterService *ClusterService) process() {
	for {
		_, alive := <-clusterService.reconnectChan
		if !alive {
			return
		}
		clusterService.reconnect()
	}
}
func (clusterService *ClusterService) reconnect() {
	err := clusterService.connectToCluster()
	if err != nil {
		log.Println("client will shutdown since it could not reconnect.")
		clusterService.client.Shutdown()
	}

}
func (clusterService *ClusterService) connectToCluster() error {

	currentAttempt := int32(1)
	attempLimit := clusterService.config.ClientNetworkConfig().ConnectionAttemptLimit()
	retryDelay := clusterService.config.ClientNetworkConfig().ConnectionAttemptPeriod()
	for currentAttempt <= attempLimit {
		currentAttempt++
		members := clusterService.members.Load().([]*Member)
		addresses := getPossibleAddresses(clusterService.config.ClientNetworkConfig().Addresses(), members)
		for _, address := range addresses {
			if !clusterService.client.LifecycleService.isLive.Load().(bool) {
				return core.NewHazelcastIllegalStateError("giving up on retrying to connect to cluster since client is shutdown.", nil)
			}
			err := clusterService.connectToAddress(&address)
			if err != nil {
				log.Println("the following error occured while trying to connect to cluster: ", err)
				if _, ok := err.(*core.HazelcastAuthenticationError); ok {
					return err
				}
				continue
			}
			return nil
		}
		if currentAttempt <= attempLimit {
			time.Sleep(time.Duration(retryDelay) * time.Second)
		}
	}
	return core.NewHazelcastIllegalStateError("could not connect to any addresses", nil)
}
func (clusterService *ClusterService) connectToAddress(address *Address) error {
	connectionChannel, errChannel := clusterService.client.ConnectionManager.GetOrConnect(address, true)
	var con *Connection
	select {
	case con = <-connectionChannel:
	case err := <-errChannel:
		return err
	}
	if !con.isOwnerConnection {
		err := clusterService.client.ConnectionManager.clusterAuthenticator(con, true)
		if err != nil {
			return err
		}
	}

	err := clusterService.initMembershipListener(con)
	if err != nil {
		return err
	}
	clusterService.client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_CONNECTED)
	return nil
}

func (clusterService *ClusterService) initMembershipListener(connection *Connection) error {
	wg.Add(1)
	request := ClientAddMembershipListenerEncodeRequest(false)
	eventHandler := func(message *ClientMessage) {
		ClientAddMembershipListenerHandle(message, clusterService.handleMember, clusterService.handleMemberList, clusterService.handleMemberAttributeChange)
	}
	invocation := NewInvocation(request, -1, nil, connection, clusterService.client)
	invocation.eventHandler = eventHandler
	response, err := clusterService.client.InvocationService.sendInvocation(invocation).Result()
	if err != nil {
		return err
	}
	registrationId := ClientAddMembershipListenerDecodeResponse(response)()
	wg.Wait() //Wait until the inital member list is fetched.
	log.Println("Registered membership listener with Id ", *registrationId)
	return nil
}
func (clusterService *ClusterService) AddListener(listener interface{}) *string {
	registrationId, _ := common.NewUUID()
	clusterService.mu.Lock()
	defer clusterService.mu.Unlock()
	listeners := clusterService.listeners.Load().(map[string]interface{})
	copyListeners := make(map[string]interface{}, len(listeners)+1)
	for k, v := range listeners {
		copyListeners[k] = v
	}
	copyListeners[registrationId] = listener
	clusterService.listeners.Store(copyListeners)
	return &registrationId
}
func (clusterService *ClusterService) RemoveListener(registrationId *string) bool {
	clusterService.mu.Lock()
	defer clusterService.mu.Unlock()
	listeners := clusterService.listeners.Load().(map[string]interface{})
	copyListeners := make(map[string]interface{}, len(listeners)-1)
	for k, v := range listeners {
		copyListeners[k] = v
	}
	_, found := copyListeners[*registrationId]
	if found {
		delete(copyListeners, *registrationId)
	}
	clusterService.listeners.Store(copyListeners)
	return found
}

func (clusterService *ClusterService) handleMember(member *Member, eventType int32) {
	if eventType == MEMBER_ADDED {
		clusterService.memberAdded(member)
	} else if eventType == MEMBER_REMOVED {
		clusterService.memberRemoved(member)
	}
	clusterService.client.PartitionService.refresh <- struct{}{}
}

func (clusterService *ClusterService) handleMemberList(members []*Member) {
	if len(members) == 0 {
		return
	}
	previousMembers := clusterService.members.Load().([]*Member)
	//TODO:: This loop is O(n^2), it is better to store members in a map to speed it up.
	for _, member := range previousMembers {
		found := false
		for _, newMember := range members {
			if *member.Address().(*Address) == *newMember.Address().(*Address) {
				found = true
				break
			}
		}
		if !found {
			clusterService.memberRemoved(member)
		}
	}
	for _, member := range members {
		found := false
		for _, previousMember := range previousMembers {
			if *member.Address().(*Address) == *previousMember.Address().(*Address) {
				found = true
				break
			}
		}
		if !found {
			clusterService.memberAdded(member)
		}
	}
	clusterService.client.PartitionService.refresh <- struct{}{}
	wg.Done() //initial member list is fetched
}
func (clusterService *ClusterService) handleMemberAttributeChange(uuid *string, key *string, operationType int32, value *string) {
	//TODO :: implement this.
}
func (clusterService *ClusterService) memberAdded(member *Member) {
	members := clusterService.members.Load().([]*Member)
	copyMembers := make([]*Member, len(members))
	for index, member := range members {
		copyMembers[index] = member
	}
	copyMembers = append(copyMembers, member)
	clusterService.members.Store(copyMembers)
	listeners := clusterService.listeners.Load().(map[string]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(MemberAddedListener); ok {
			listener.(MemberAddedListener).MemberAdded(member)
		}
	}
}
func (clusterService *ClusterService) memberRemoved(member *Member) {
	members := clusterService.members.Load().([]*Member)
	copyMembers := make([]*Member, len(members)-1)
	index := 0
	for _, curMember := range members {
		if !curMember.Equal(*member) {
			copyMembers[index] = curMember
			index++
		}
	}
	clusterService.members.Store(copyMembers)
	connection := clusterService.client.ConnectionManager.getActiveConnection(member.Address().(*Address))
	if connection != nil {
		connection.Close(core.NewHazelcastTargetDisconnectedError("the client"+
			"has closed the connection to this member after receiving a member left event from the cluster", nil))
	}
	listeners := clusterService.listeners.Load().(map[string]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(MemberRemovedListener); ok {
			listener.(MemberRemovedListener).MemberRemoved(member)
		}
	}
}
func (clusterService *ClusterService) GetMemberList() []core.IMember {
	membersList := clusterService.members.Load().([]*Member)
	members := make([]core.IMember, len(membersList))
	for index := 0; index < len(membersList); index++ {
		copyMember := *membersList[index]
		members[index] = core.IMember(&copyMember)
	}
	return members
}
func (clusterService *ClusterService) GetMember(address core.IAddress) core.IMember {
	membersList := clusterService.members.Load().([]*Member)
	for _, member := range membersList {
		if *member.Address().(*Address) == *address.(*Address) {
			copyMember := *member
			return &copyMember
		}
	}
	return nil
}

func (clusterService *ClusterService) GetMemberByUuid(uuid string) core.IMember {
	membersList := clusterService.members.Load().([]*Member)
	for _, member := range membersList {
		if member.Uuid() == uuid {
			copyMember := *member
			return &copyMember
		}
	}
	return nil
}
func (clusterService *ClusterService) onConnectionClosed(connection *Connection, cause error) {
	ownerConnectionAddress := clusterService.ownerConnectionAddress.Load().(*Address)
	if connection.endpoint.Load().(*Address).Host() != "" && ownerConnectionAddress.Host() != "" &&
		*connection.endpoint.Load().(*Address) == *ownerConnectionAddress && clusterService.client.LifecycleService.isLive.Load().(bool) {
		clusterService.client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_DISCONNECTED)
		clusterService.ownerConnectionAddress.Store(&Address{})
		clusterService.reconnectChan <- struct{}{}
	}
}
func (clusterSerice *ClusterService) onConnectionOpened(connection *Connection) {

}
