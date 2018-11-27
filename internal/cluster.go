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
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/util/iputil"
)

const (
	defaultAddress       = "127.0.0.1"
	defaultPort          = 5701
	memberAdded    int32 = 1
	memberRemoved  int32 = 2
)

type clusterService struct {
	client                 *HazelcastClient
	config                 *config.Config
	members                atomic.Value
	ownerUUID              atomic.Value
	uuid                   atomic.Value
	ownerConnectionAddress atomic.Value
	listeners              atomic.Value
	addressProviders       []AddressProvider
	mu                     sync.Mutex
	memberWg               *sync.WaitGroup
	reconnectChan          chan struct{}
	cancelChan             chan struct{}
}

func newClusterService(client *HazelcastClient, addressProviders []AddressProvider) *clusterService {
	service := &clusterService{
		client: client, config: client.Config, reconnectChan: make(chan struct{}, 1),
		cancelChan: make(chan struct{}, 1),
	}
	service.init()
	service.registerMembershipListeners()
	service.addressProviders = addressProviders
	service.client.ConnectionManager.addListener(service)
	go service.process()
	return service
}

func (cs *clusterService) init() {
	cs.ownerConnectionAddress.Store(&proto.Address{})
	cs.members.Store(make([]*proto.Member, 0))       //Initialize
	cs.listeners.Store(make(map[string]interface{})) //Initialize
	cs.memberWg = new(sync.WaitGroup)
	cs.ownerUUID.Store("") //Initialize
	cs.uuid.Store("")      //Initialize
}

func (cs *clusterService) registerMembershipListeners() {
	for _, membershipListener := range cs.config.MembershipListeners() {
		cs.AddMembershipListener(membershipListener)
	}
}

func (cs *clusterService) start() error {
	return cs.connectToCluster()
}

func (cs *clusterService) getPossibleMemberAddresses() []core.Address {
	seen := make(map[proto.Address]struct{})
	addrs := make([]core.Address, 0)
	memberList := cs.GetMembers()

	for _, member := range memberList {
		a := *member.Address().(*proto.Address)
		if _, found := seen[a]; !found {
			addrs = append(addrs, member.Address())
			seen[a] = struct{}{}
		}
	}

	for _, provider := range cs.addressProviders {
		providerAddrs := provider.LoadAddresses()
		for _, addr := range providerAddrs {
			a := *addr.(*proto.Address)
			if _, found := seen[a]; !found {
				addrs = append(addrs, addr)
				seen[a] = struct{}{}
			}
		}
	}
	return addrs
}

func createAddressFromString(addressList []string) []proto.Address {
	if addressList == nil {
		addressList = make([]string, 0)
	}
	allAddresses := make(map[proto.Address]struct{}, len(addressList))
	for _, address := range addressList {
		ip, port := iputil.GetIPAndPort(address)
		if iputil.IsValidIPAddress(ip) {
			if port == -1 {
				allAddresses[*proto.NewAddressWithParameters(ip, defaultPort)] = struct{}{}
				allAddresses[*proto.NewAddressWithParameters(ip, defaultPort+1)] = struct{}{}
				allAddresses[*proto.NewAddressWithParameters(ip, defaultPort+2)] = struct{}{}
			} else {
				allAddresses[*proto.NewAddressWithParameters(ip, port)] = struct{}{}
			}

		}
	}
	addresses := make([]proto.Address, len(allAddresses))
	index := 0
	for k := range allAddresses {
		addresses[index] = k
		index++
	}
	if len(addresses) == 0 {
		addresses = append(addresses, *proto.NewAddressWithParameters(defaultAddress, defaultPort))
	}
	return addresses
}

func (cs *clusterService) process() {
	for {
		select {
		case <-cs.reconnectChan:
			cs.reconnect()
		case <-cs.cancelChan:
			return
		}
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
	attempLimit := cs.config.NetworkConfig().ConnectionAttemptLimit()
	retryDelay := cs.config.NetworkConfig().ConnectionAttemptPeriod()
	for currentAttempt < attempLimit {
		currentAttempt++
		addresses := cs.getPossibleMemberAddresses()
		for _, address := range addresses {
			if !cs.client.lifecycleService.isLive.Load().(bool) {
				return core.NewHazelcastIllegalStateError("giving up on retrying to connect to cluster since client is shutdown.", nil)
			}
			err := cs.connectToAddress(address)
			if err != nil {
				log.Println("The following error occurred while trying to connect to:", address, "in cluster. attempt ",
					currentAttempt, " of ", attempLimit, " error: ", err)
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

func (cs *clusterService) connectToAddress(address core.Address) error {
	connection, err := cs.client.ConnectionManager.getOrConnect(address, true)
	if err != nil {
		return err
	}

	err = cs.initMembershipListener(connection)
	if err != nil {
		return err
	}
	cs.client.lifecycleService.fireLifecycleEvent(core.LifecycleStateConnected)
	return nil
}

func (cs *clusterService) initMembershipListener(connection *Connection) error {
	cs.memberWg.Add(1)
	request := proto.ClientAddMembershipListenerEncodeRequest(false)
	eventHandler := func(message *proto.ClientMessage) {
		proto.ClientAddMembershipListenerHandle(message, cs.handleMember, cs.handleMemberList, cs.handleMemberAttributeChange)
	}
	invocation := newInvocation(request, -1, nil, connection, cs.client)
	invocation.eventHandler = eventHandler
	response, err := cs.client.InvocationService.sendInvocation(invocation).Result()
	if err != nil {
		return err
	}
	registrationID := proto.ClientAddMembershipListenerDecodeResponse(response)()
	cs.memberWg.Wait() // Wait until the initial member list is fetched.
	cs.logMembers()
	log.Println("Registered membership listener with ID ", registrationID)
	return nil
}

func (cs *clusterService) logMembers() {
	members := cs.members.Load().([]*proto.Member)
	membersInfo := fmt.Sprintf("\n\nMembers {size:%d} [\n", len(members))
	for _, member := range members {
		memberInfo := fmt.Sprint("\t", member)
		memberInfo += "\n"
		membersInfo += memberInfo
	}
	membersInfo += "]\n"
	log.Println(membersInfo)
}

func (cs *clusterService) AddMembershipListener(listener interface{}) string {
	registrationID, _ := iputil.NewUUID()
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

func (cs *clusterService) RemoveMembershipListener(registrationID string) bool {
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

func (cs *clusterService) handleMember(member *proto.Member, eventType int32) {
	if eventType == memberAdded {
		cs.memberAdded(member)
	} else if eventType == memberRemoved {
		cs.memberRemoved(member)
	}
	cs.logMembers()
	cs.client.PartitionService.refresh <- struct{}{}
}

func (cs *clusterService) handleMemberList(members []*proto.Member) {
	if len(members) == 0 {
		return
	}
	previousMembers := cs.members.Load().([]*proto.Member)
	//TODO:: This loop is O(n^2), it is better to store members in a map to speed it up.
	for _, member := range previousMembers {
		found := false
		for _, newMember := range members {
			if *member.Address().(*proto.Address) == *newMember.Address().(*proto.Address) {
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
			if *member.Address().(*proto.Address) == *previousMember.Address().(*proto.Address) {
				found = true
				break
			}
		}
		if !found {
			cs.memberAdded(member)
		}
	}
	cs.client.PartitionService.refresh <- struct{}{}
	cs.memberWg.Done() //initial member list is fetched
}

func (cs *clusterService) handleMemberAttributeChange(uuid string, key string, operationType int32, value string) {
	//TODO :: implement this.
}

func (cs *clusterService) memberAdded(member *proto.Member) {
	members := cs.members.Load().([]*proto.Member)
	copyMembers := make([]*proto.Member, len(members))
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

func (cs *clusterService) memberRemoved(member *proto.Member) {
	members := cs.members.Load().([]*proto.Member)
	copyMembers := make([]*proto.Member, 0, len(members)-1)
	for _, curMember := range members {
		if !curMember.Equal(*member) {
			copyMembers = append(copyMembers, curMember)
		}
	}
	cs.members.Store(copyMembers)
	connection := cs.client.ConnectionManager.getActiveConnection(member.Address().(*proto.Address))
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

func (cs *clusterService) GetMembers() []core.Member {
	membersList := cs.members.Load().([]*proto.Member)
	members := make([]core.Member, len(membersList))
	for index := 0; index < len(membersList); index++ {
		members[index] = core.Member(membersList[index])
	}
	return members
}

func (cs *clusterService) GetMembersWithSelector(selector core.MemberSelector) (members []core.Member) {
	if selector == nil {
		return cs.GetMembers()
	}
	membersList := cs.members.Load().([]*proto.Member)
	for _, member := range membersList {
		if selector.Select(member) {
			members = append(members, core.Member(member))
		}
	}
	return
}

func (cs *clusterService) GetMember(address core.Address) core.Member {
	if address == nil {
		return nil
	}
	membersList := cs.members.Load().([]*proto.Member)
	for _, member := range membersList {
		if *member.Address().(*proto.Address) == *address.(*proto.Address) {
			return member
		}
	}
	return nil
}

func (cs *clusterService) GetMemberByUUID(uuid string) core.Member {
	membersList := cs.members.Load().([]*proto.Member)
	for _, member := range membersList {
		if member.UUID() == uuid {
			return member
		}
	}
	return nil
}

func (cs *clusterService) getOwnerConnectionAddress() *proto.Address {
	address, ok := cs.ownerConnectionAddress.Load().(*proto.Address)
	if ok {
		return address
	}
	return nil
}

func (cs *clusterService) onConnectionClosed(connection *Connection, cause error) {
	address, ok := connection.endpoint.Load().(*proto.Address)
	ownerConnectionAddress := cs.getOwnerConnectionAddress()
	if ok && ownerConnectionAddress != nil &&
		*address == *ownerConnectionAddress && cs.client.lifecycleService.isLive.Load().(bool) {
		cs.client.lifecycleService.fireLifecycleEvent(core.LifecycleStateDisconnected)
		cs.ownerConnectionAddress.Store(&proto.Address{})
		cs.reconnectChan <- struct{}{}
	}
}

func (cs *clusterService) onConnectionOpened(connection *Connection) {

}

func (cs *clusterService) shutdown() {
	cs.cancelChan <- struct{}{}
}
