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
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/IPutil"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

const (
	defaultAddress       = "127.0.0.1"
	defaultPort          = 5701
	memberAdded    int32 = 1
	memberRemoved  int32 = 2
)

var wg sync.WaitGroup

type clusterService struct {
	client                 *HazelcastClient
	config                 *config.ClientConfig
	members                atomic.Value
	ownerUUID              atomic.Value
	uuid                   atomic.Value
	ownerConnectionAddress atomic.Value
	listeners              atomic.Value
	mu                     sync.Mutex
	reconnectChan          chan struct{}
}

func newClusterService(client *HazelcastClient, config *config.ClientConfig) *clusterService {
	service := &clusterService{client: client, config: config, reconnectChan: make(chan struct{}, 1)}
	service.ownerConnectionAddress.Store(&protocol.Address{})
	service.members.Store(make([]*protocol.Member, 0))    //Initialize
	service.listeners.Store(make(map[string]interface{})) //Initialize
	ownerUUID := ""
	service.ownerUUID.Store(ownerUUID) //Initialize
	uuid := ""
	service.uuid.Store(uuid) //Initialize
	for _, membershipListener := range client.ClientConfig.MembershipListeners() {
		service.AddListener(membershipListener)
	}
	service.client.ConnectionManager.addListener(service)
	go service.process()
	return service
}

func (cs *clusterService) start() error {
	return cs.connectToCluster()
}

func getPossibleAddresses(addressList []string, memberList []*protocol.Member) []protocol.Address {
	if addressList == nil {
		addressList = make([]string, 0)
	}
	if memberList == nil {
		memberList = make([]*protocol.Member, 0)
	}
	allAddresses := make(map[protocol.Address]struct{}, len(addressList)+len(memberList))
	for _, address := range addressList {
		ip, port := IPutil.GetIPAndPort(address)
		if IPutil.IsValidIPAddress(ip) {
			if port == -1 {
				allAddresses[*protocol.NewAddressWithParameters(ip, defaultPort)] = struct{}{}
				allAddresses[*protocol.NewAddressWithParameters(ip, defaultPort+1)] = struct{}{}
				allAddresses[*protocol.NewAddressWithParameters(ip, defaultPort+2)] = struct{}{}
			} else {
				allAddresses[*protocol.NewAddressWithParameters(ip, port)] = struct{}{}
			}

		}
	}
	for _, member := range memberList {
		allAddresses[*member.Address().(*protocol.Address)] = struct{}{}
	}
	addresses := make([]protocol.Address, len(allAddresses))
	index := 0
	for k := range allAddresses {
		addresses[index] = k
		index++
	}
	if len(addresses) == 0 {
		addresses = append(addresses, *protocol.NewAddressWithParameters(defaultAddress, defaultPort))
	}
	return addresses
}

func (cs *clusterService) process() {
	for {
		_, alive := <-cs.reconnectChan
		if !alive {
			return
		}
		cs.reconnect()
	}
}

func (cs *clusterService) reconnect() {
	err := cs.connectToCluster()
	if err != nil {
		cs.client.Shutdown()
		log.Println("Client will shutdown since it could not reconnect.")
	}

}

func (cs *clusterService) connectToCluster() error {

	currentAttempt := int32(0)
	attempLimit := cs.config.ClientNetworkConfig().ConnectionAttemptLimit()
	retryDelay := cs.config.ClientNetworkConfig().ConnectionAttemptPeriod()
	for currentAttempt < attempLimit {
		currentAttempt++
		members := cs.members.Load().([]*protocol.Member)
		addresses := getPossibleAddresses(cs.config.ClientNetworkConfig().Addresses(), members)
		for _, address := range addresses {
			if !cs.client.LifecycleService.isLive.Load().(bool) {
				return core.NewHazelcastIllegalStateError("giving up on retrying to connect to cluster since client is shutdown.", nil)
			}
			err := cs.connectToAddress(&address)
			if err != nil {
				log.Println("The following error occurred while trying to connect to cluster. attempt ",
					currentAttempt, " of ", attempLimit, " error: ", err)
				if _, ok := err.(*core.HazelcastAuthenticationError); ok {
					return err
				}
				continue
			}
			return nil
		}
		if currentAttempt <= attempLimit {
			time.Sleep(retryDelay)
		}
	}
	return core.NewHazelcastIllegalStateError("could not connect to any addresses", nil)
}

func (cs *clusterService) connectToAddress(address *protocol.Address) error {
	connectionChannel, errChannel := cs.client.ConnectionManager.getOrConnect(address, true)
	var con *Connection
	select {
	case con = <-connectionChannel:
	case err := <-errChannel:
		return err
	}

	err := cs.initMembershipListener(con)
	if err != nil {
		return err
	}
	cs.client.LifecycleService.fireLifecycleEvent(LifecycleStateConnected)
	return nil
}

func (cs *clusterService) initMembershipListener(connection *Connection) error {
	wg.Add(1)
	request := protocol.ClientAddMembershipListenerEncodeRequest(false)
	eventHandler := func(message *protocol.ClientMessage) {
		protocol.ClientAddMembershipListenerHandle(message, cs.handleMember, cs.handleMemberList, cs.handleMemberAttributeChange)
	}
	invocation := newInvocation(request, -1, nil, connection, cs.client)
	invocation.eventHandler = eventHandler
	response, err := cs.client.InvocationService.sendInvocation(invocation).Result()
	if err != nil {
		return err
	}
	registrationID := protocol.ClientAddMembershipListenerDecodeResponse(response)()
	wg.Wait() // Wait until the initial member list is fetched.
	cs.logMembers()
	log.Println("Registered membership listener with ID ", registrationID)
	return nil
}

func (cs *clusterService) logMembers() {
	members := cs.members.Load().([]*protocol.Member)
	membersInfo := fmt.Sprintf("\n\nMembers {size:%d} [\n", len(members))
	for _, member := range members {
		memberInfo := fmt.Sprint("\t", member)
		memberInfo += "\n"
		membersInfo += memberInfo
	}
	membersInfo += "]\n"
	log.Println(membersInfo)
}

func (cs *clusterService) AddListener(listener interface{}) string {
	registrationID, _ := IPutil.NewUUID()
	cs.mu.Lock()
	defer cs.mu.Unlock()
	listeners := cs.listeners.Load().(map[string]interface{})
	copyListeners := make(map[string]interface{}, len(listeners)+1)
	for k, v := range listeners {
		copyListeners[k] = v
	}
	copyListeners[registrationID] = listener
	cs.listeners.Store(copyListeners)
	return registrationID
}

func (cs *clusterService) RemoveListener(registrationID string) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	listeners := cs.listeners.Load().(map[string]interface{})
	copyListeners := make(map[string]interface{}, len(listeners)-1)
	for k, v := range listeners {
		copyListeners[k] = v
	}
	_, found := copyListeners[registrationID]
	if found {
		delete(copyListeners, registrationID)
	}
	cs.listeners.Store(copyListeners)
	return found
}

func (cs *clusterService) handleMember(member *protocol.Member, eventType int32) {
	if eventType == memberAdded {
		cs.memberAdded(member)
	} else if eventType == memberRemoved {
		cs.memberRemoved(member)
	}
	cs.logMembers()
	cs.client.PartitionService.refresh <- struct{}{}
}

func (cs *clusterService) handleMemberList(members []*protocol.Member) {
	if len(members) == 0 {
		return
	}
	previousMembers := cs.members.Load().([]*protocol.Member)
	//TODO:: This loop is O(n^2), it is better to store members in a map to speed it up.
	for _, member := range previousMembers {
		found := false
		for _, newMember := range members {
			if *member.Address().(*protocol.Address) == *newMember.Address().(*protocol.Address) {
				found = true
				break
			}
		}
		if !found {
			cs.memberRemoved(member)
		}
	}
	for _, member := range members {
		found := false
		for _, previousMember := range previousMembers {
			if *member.Address().(*protocol.Address) == *previousMember.Address().(*protocol.Address) {
				found = true
				break
			}
		}
		if !found {
			cs.memberAdded(member)
		}
	}
	cs.client.PartitionService.refresh <- struct{}{}
	wg.Done() //initial member list is fetched
}

func (cs *clusterService) handleMemberAttributeChange(uuid string, key string, operationType int32, value string) {
	//TODO :: implement this.
}

func (cs *clusterService) memberAdded(member *protocol.Member) {
	members := cs.members.Load().([]*protocol.Member)
	copyMembers := make([]*protocol.Member, len(members))
	copy(copyMembers, members)
	copyMembers = append(copyMembers, member)
	cs.members.Store(copyMembers)
	listeners := cs.listeners.Load().(map[string]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(core.MemberAddedListener); ok {
			listener.(core.MemberAddedListener).MemberAdded(member)
		}
	}
}

func (cs *clusterService) memberRemoved(member *protocol.Member) {
	members := cs.members.Load().([]*protocol.Member)
	copyMembers := make([]*protocol.Member, len(members)-1)
	index := 0
	for _, curMember := range members {
		if !curMember.Equal(*member) {
			copyMembers[index] = curMember
			index++
		}
	}
	cs.members.Store(copyMembers)
	connection := cs.client.ConnectionManager.getActiveConnection(member.Address().(*protocol.Address))
	if connection != nil {
		connection.close(core.NewHazelcastTargetDisconnectedError("the client"+
			"has closed the Connection to this member after receiving a member left event from the cluster", nil))
	}
	listeners := cs.listeners.Load().(map[string]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(core.MemberRemovedListener); ok {
			listener.(core.MemberRemovedListener).MemberRemoved(member)
		}
	}
}

func (cs *clusterService) GetMemberList() []core.Member {
	membersList := cs.members.Load().([]*protocol.Member)
	members := make([]core.Member, len(membersList))
	for index := 0; index < len(membersList); index++ {
		members[index] = core.Member(membersList[index])
	}
	return members
}

func (cs *clusterService) GetMembersWithSelector(selector core.MemberSelector) (members []core.Member) {
	if selector == nil {
		return cs.GetMemberList()
	}
	membersList := cs.members.Load().([]*protocol.Member)
	for _, member := range membersList {
		if selector.Select(member) {
			members = append(members, core.Member(member))
		}
	}
	return
}

func (cs *clusterService) GetMember(address core.Address) core.Member {
	membersList := cs.members.Load().([]*protocol.Member)
	for _, member := range membersList {
		if *member.Address().(*protocol.Address) == *address.(*protocol.Address) {
			return member
		}
	}
	return nil
}

func (cs *clusterService) GetMemberByUUID(uuid string) core.Member {
	membersList := cs.members.Load().([]*protocol.Member)
	for _, member := range membersList {
		if member.UUID() == uuid {
			return member
		}
	}
	return nil
}

func (cs *clusterService) onConnectionClosed(connection *Connection, cause error) {
	ownerConnectionAddress := cs.ownerConnectionAddress.Load().(*protocol.Address)
	if connection.endpoint.Load().(*protocol.Address).Host() != "" && ownerConnectionAddress.Host() != "" &&
		*connection.endpoint.Load().(*protocol.Address) == *ownerConnectionAddress && cs.client.LifecycleService.isLive.Load().(bool) {
		cs.client.LifecycleService.fireLifecycleEvent(LifecycleStateDisconnected)
		cs.ownerConnectionAddress.Store(&protocol.Address{})
		cs.reconnectChan <- struct{}{}
	}
}

func (cs *clusterService) onConnectionOpened(connection *Connection) {

}

func (cs *clusterService) shutdown() {
	close(cs.reconnectChan)
}
