// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/core/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/util/iputil"
)

const (
	defaultAddress              = "127.0.0.1"
	defaultPort                 = 5701
	memberAdded           int32 = 1
	memberRemoved         int32 = 2
	InitialMembersTimeout       = 120 * time.Second
)

var EmptySnapshot = NewEmptyMemberListSnapshot()

type clusterService struct {
	client                  *HazelcastClient
	config                  *config.Config
	members                 atomic.Value
	ownerUUID               atomic.Value
	uuid                    atomic.Value
	ownerConnectionAddress  atomic.Value
	listeners               sync.Map
	removeListenerMu        sync.Mutex
	addressProviders        []AddressProvider
	initialMemberListWg     *sync.WaitGroup
	reconnectChan           chan struct{}
	cancelChan              chan struct{}
	listenersV2             sync.Map
	memberListSnapshot      atomic.Value
	labels                  []string
	initialListFetchedLatch TimedWaitGroup
	clusterViewLockMu       sync.Mutex
	logger                  logger.Logger
}

func newClusterService(client *HazelcastClient, addressProviders []AddressProvider) *clusterService {
	service := &clusterService{
		client: client, config: client.Config, reconnectChan: make(chan struct{}, 1),
		cancelChan: make(chan struct{}, 1),
	}
	service.init()
	service.registerMembershipListeners()
	service.logger = client.logger
	service.addressProviders = addressProviders
	service.client.ConnectionManager.addListener(service)
	go service.process()
	return service
}

func (cs *clusterService) init() {
	cs.ownerConnectionAddress.Store(&proto.Address{})
	cs.members.Store(make([]*proto.Member, 0)) //Initialize
	cs.initialMemberListWg = new(sync.WaitGroup)
	cs.ownerUUID.Store("") //Initialize
	cs.uuid.Store("")      //Initialize
	cs.memberListSnapshot.Store(EmptySnapshot)
	cs.labels = cs.client.Config.GetLabels()
	cs.initialListFetchedLatch.Add(1)
}

func (cs *clusterService) GetMemberV2(uuid core.UUID) core.Member {
	return cs.memberListSnapshot.Load().(MemberListSnapshot).GetMembers()[uuid]
}

func (cs *clusterService) GetMemberList() []core.Member {
	memberListSnapshot := cs.memberListSnapshot.Load().(MemberListSnapshot)
	members := make([]core.Member, 0, len(memberListSnapshot.GetMembers()))
	for _, member := range memberListSnapshot.GetMembers() {
		members = append(members, member)
	}
	return members
}

func (cs *clusterService) GetMembersV2(selector core.MemberSelector) []core.Member {
	if selector == nil {
		return cs.GetMembers()
	}
	membersList := cs.memberListSnapshot.Load().(MemberListSnapshot)
	members := make([]core.Member, 0)
	for _, member := range membersList.GetMembers() {
		if selector.Select(member) {
			members = append(members, member)
		}
	}
	return members
}

func (cs *clusterService) GetSize() int {
	membersList := cs.memberListSnapshot.Load().(MemberListSnapshot)
	return len(membersList.GetMembers())
}

func (cs *clusterService) GetLocalClient() core.ClientInfo {
	connectionManager := cs.client.ConnectionManager
	connection := connectionManager.getRandomConnection()
	var localAddress net.Addr
	if connection != nil {
		localAddress = connection.localAddress()
	} else {
		localAddress = nil
	}
	clientName := cs.client.Name()
	uuid := connectionManager.getClientUUID()
	return core.ClientInfo{UUID: uuid, LocalAddress: localAddress, ClientType: proto.ClientType, Name: clientName, Labels: cs.labels}
}

func (cs *clusterService) AddMembershipListener(listener core.MembershipListener) string {
	cs.clusterViewLockMu.Lock()
	defer cs.clusterViewLockMu.Unlock()
	registrationId := core.NewUUID().ToString()
	cs.listenersV2.Store(registrationId, listener)

	if isInitialMembershipListener(listener) {
		members := cs.GetMemberList()
		if len(members) == 0 {
			// if members are empty,it means initial event did not arrive yet
			// it will be redirected to listeners when it arrives see #handleInitialMembershipEvent
			membershipEvent := NewInitialMembershipEvent(members)
			listener.(core.InitialMembershipListener).Init(membershipEvent)
		}
	}
	return registrationId
}

func isInitialMembershipListener(listener core.MembershipListener) bool {
	initialMembershipListener := reflect.TypeOf((*core.InitialMembershipListener)(nil)).Elem()
	return reflect.TypeOf(&listener).Implements(initialMembershipListener)
}

func (cs *clusterService) RemoveMembershipListenerV2(uuid string) bool {
	cs.listenersV2.Delete(uuid)
	_, ok := cs.listenersV2.Load(uuid)
	return !ok
}

func (cs *clusterService) startV2(configuredListeners []core.MembershipListener) {
	for _, eachMembershipListener := range configuredListeners {
		cs.AddMembershipListener(eachMembershipListener)
	}
}

func (cs *clusterService) waitForInitialMemberList() error {
	await := cs.initialListFetchedLatch.Await(InitialMembersTimeout)
	if !await {
		return core.NewHazelcastIllegalStateError("Could not get initial member list from the cluster!", nil)
	}
	return nil
}

func (cs *clusterService) handleMembersViewEvent(memberListVersion int32, memberInfos []proto.MemberInfo) {
	memberListSnapshot := cs.memberListSnapshot.Load().(MemberListSnapshot)
	if reflect.DeepEqual(EmptySnapshot, memberListSnapshot) {
		cs.clusterViewLockMu.Lock()
		//this means this is the first time client connected to cluster
		cs.applyInitialState(memberListVersion, memberInfos)
		cs.initialListFetchedLatch.Done()
		cs.clusterViewLockMu.Unlock()
		return
	}

	membershipEvents := make([]core.MembershipEvent, 0)
	if memberListVersion >= memberListSnapshot.GetVersion() {
		cs.clusterViewLockMu.Lock()
		prevMembers := memberListSnapshot.GetMembers()
		snapshot := createSnapshot(memberListVersion, memberInfos)
		cs.memberListSnapshot.Store(snapshot)
		currentMembers := snapshot.GetMemberList()
		membershipEvents = append(membershipEvents, cs.detectMembershipEvents(prevMembers, currentMembers)...)
		cs.clusterViewLockMu.Unlock()
	}

	cs.fireEvents(membershipEvents)
}

func (cs *clusterService) applyInitialState(memberListVersion int32, memberInfos []proto.MemberInfo) {
	snapshot := createSnapshot(memberListVersion, memberInfos)
	cs.memberListSnapshot.Store(snapshot)
	cs.logger.Info(membersString(snapshot))
	initialMembershipEvent := NewInitialMembershipEvent(snapshot.GetMemberList())

	cs.listenersV2.Range(func(key, value interface{}) bool {
		listener := value.(core.MembershipListener)
		if isInitialMembershipListener(listener) {
			listener.(core.InitialMembershipListener).Init(initialMembershipEvent)
		}
		return true
	})
}

func membersString(snapshot MemberListSnapshot) string {
	var sb strings.Builder
	sb.WriteString("\n\nMembers [")
	sb.WriteString(strconv.Itoa(len(snapshot.GetMembers())))
	sb.WriteString("] {")
	for _, member := range snapshot.GetMembers() {
		sb.WriteString("\n\t")
		sb.WriteString(member.String())
	}
	sb.WriteString("\n}\n")
	return sb.String()
}

func createSnapshot(memberListVersion int32, memberInfos []proto.MemberInfo) MemberListSnapshot {
	newMembers := make(map[core.UUID]core.Member, len(memberInfos))

	for _, memberInfo := range memberInfos {
		uuid := memberInfo.GetUuid()
		member := proto.NewMember(memberInfo.GetAddress(), uuid, memberInfo.GetLiteMember(),
			memberInfo.GetAttributes(), memberInfo.GetVersion(), memberInfo.GetAddressMap())
		newMembers[uuid] = member
	}

	return NewMemberListSnapshot(memberListVersion, newMembers)
}

func (cs *clusterService) detectMembershipEvents(prevMembers map[core.UUID]core.Member, currentMembers []core.Member) []core.MembershipEvent {
	newMembers := make([]core.Member, 0)
	deadMembers := make(map[core.UUID]core.Member)
	for _, eachMember := range prevMembers {
		deadMembers[eachMember.UUID()] = eachMember
	}

	for _, eachMember := range currentMembers {
		member, ok := deadMembers[eachMember.UUID()]
		if !ok {
			newMembers = append(newMembers, member)
		} else {
			delete(deadMembers, eachMember.UUID())
		}
	}

	membershipEvents := make([]core.MembershipEvent, 0)
	for _, eachMember := range deadMembers {
		membershipEvents = append(membershipEvents, NewMembershipEvent(eachMember, currentMembers, core.MemberEventRemoved))
		connection, ok := cs.client.ConnectionManager.getActiveConnections()[eachMember.UUID().ToString()]
		if ok {
			connection.close(core.NewHazelcastTargetDisconnectedError("The client has closed the connection to this"+
				"member, after receiving a member left event from the cluster", nil))
		}
	}

	for _, eachMember := range newMembers {
		membershipEvents = append(membershipEvents, NewMembershipEvent(eachMember, currentMembers, core.MemberEventRemoved))
	}

	if len(membershipEvents) != 0 {
		memberListSnapshot := cs.memberListSnapshot.Load().(MemberListSnapshot)
		if len(memberListSnapshot.GetMembers()) != 0 {
			cs.logger.Info("ClusterService" + membersString(memberListSnapshot))
		}
	}

	return membershipEvents
}

func (cs *clusterService) fireEvents(events []core.MembershipEvent) {
	for _, event := range events {
		cs.listenersV2.Range(func(key, value interface{}) bool {
			listener := value.(core.MembershipListener)
			if event.GetEventType() == core.MemberEventAdded {
				listener.MemberAdded(event)
			} else if event.GetEventType() == core.MemberEventRemoved {
				listener.MemberRemoved(event)
			}
			return true
		})
	}
}

func (cs *clusterService) clearMemberListVersion() {
	clusterViewSnapshot := cs.memberListSnapshot.Load().(MemberListSnapshot)
	if !reflect.DeepEqual(clusterViewSnapshot, EmptySnapshot) {
		cs.memberListSnapshot.Store(NewMemberListSnapshot(0, clusterViewSnapshot.GetMembers()))
	}
}

func (cs *clusterService) Reset() {
	cs.logger.Debug("ClusterService Resetting the cluster snapshot.")
	cs.initialListFetchedLatch = TimedWaitGroup{}
	cs.initialListFetchedLatch.Add(1)
	cs.memberListSnapshot.Store(EmptySnapshot)
}

func (cs *clusterService) registerMembershipListeners() {
	/*for _, membershipListener := range cs.config.MembershipListeners() {
		cs.AddMembershipListener(membershipListener)
	}*/
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

func createAddressesFromString(addressesInString []string) []proto.Address {
	if addressesInString == nil {
		addressesInString = make([]string, 0)
	}
	addressesSet := make(map[proto.Address]struct{}, len(addressesInString))
	for _, address := range addressesInString {
		ip, port := iputil.GetIPAndPort(address)
		if port == -1 {
			addressesSet[*proto.NewAddressWithParameters(ip, defaultPort)] = struct{}{}
			addressesSet[*proto.NewAddressWithParameters(ip, defaultPort+1)] = struct{}{}
			addressesSet[*proto.NewAddressWithParameters(ip, defaultPort+2)] = struct{}{}
		} else {
			addressesSet[*proto.NewAddressWithParameters(ip, port)] = struct{}{}
		}
	}
	addresses := make([]proto.Address, len(addressesSet))
	index := 0
	for k := range addressesSet {
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
		cs.logger.Error("Client will shutdown since it could not reconnect.")
		cs.client.Shutdown()
	}

}

func (cs *clusterService) connectToCluster() error {
	currentAttempt := int32(0)
	attemptLimit := cs.config.NetworkConfig().ConnectionAttemptLimit()
	retryDelay := cs.config.NetworkConfig().ConnectionAttemptPeriod()
	for currentAttempt < attemptLimit {
		currentAttempt++
		shouldTerminate, err := cs.connectToPossibleAddresses(currentAttempt, attemptLimit)
		if err == nil {
			return nil
		}
		if shouldTerminate {
			return err
		}
		if currentAttempt <= attemptLimit {
			time.Sleep(retryDelay)
		}
	}
	return core.NewHazelcastIllegalStateError("could not connect to any addresses", nil)
}

func (cs *clusterService) connectToPossibleAddresses(currentAttempt, attemptLimit int32) (bool, error) {
	addresses := cs.getPossibleMemberAddresses()
	for _, address := range addresses {
		if !cs.client.lifecycleService.isLive.Load().(bool) {
			return true, core.NewHazelcastIllegalStateError("giving up on retrying to connect to "+
				"cluster since client is shutdown.", nil)
		}
		err := cs.connectToAddress(address)
		if err != nil {
			cs.logger.Debug("The following error occurred while trying to connect to:", address, "in cluster. attempt ",
				currentAttempt, " of ", attemptLimit, " error: ", err)
			continue
		}
		return false, nil
	}
	return false, core.NewHazelcastIllegalStateError("could not connect to any addresses", nil)
}

func (cs *clusterService) connectToAddress(address core.Address) error {
	cs.logger.Info("Trying to connect to", address, "as owner member.")
	_, err := cs.client.ConnectionManager.getOrConnect(address, true)
	if err != nil {
		cs.logger.Warn("Error during initial connection to", address, "error:", err)
		return err
	}

	/*	err = cs.initMembershipListener(connection)
		if err != nil {
			return err
		}
	*/
	cs.client.lifecycleService.fireLifecycleEvent(core.LifecycleStateConnected)
	return nil
}

func (cs *clusterService) initMembershipListener(connection *Connection) error {
	cs.initialMemberListWg.Add(1)
	invocation := cs.createMembershipInvocation(connection)
	response, err := cs.client.InvocationService.sendInvocation(invocation).Result()
	if err != nil {
		return err
	}
	registrationID := proto.ClientAddMembershipListenerDecodeResponse(response)()
	cs.initialMemberListWg.Wait()
	cs.logMembers()
	cs.logger.Info("Registered membership listener with ID", registrationID)
	return nil
}

func (cs *clusterService) createMembershipInvocation(connection *Connection) *invocation {
	request := proto.ClientAddMembershipListenerEncodeRequest(false)
	eventHandler := func(message *proto.ClientMessage) {
		proto.ClientAddMembershipListenerHandle(message, cs.handleMember, cs.handleMemberList,
			cs.handleMemberAttributeChange)
	}
	invocation := newInvocation(request, -1, nil, connection, cs.client)
	invocation.eventHandler = eventHandler
	return invocation
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
	cs.logger.Info(membersInfo)
}

func (cs *clusterService) RemoveMembershipListener(registrationID string) bool {
	cs.removeListenerMu.Lock()
	defer cs.removeListenerMu.Unlock()
	_, found := cs.listeners.Load(registrationID)
	cs.listeners.Delete(registrationID)
	return found
}

func (cs *clusterService) handleMember(member *proto.Member, eventType int32) {
	if eventType == memberAdded {
		cs.memberAdded(member)
	} else if eventType == memberRemoved {
		cs.memberRemoved(member)
	}
	cs.logMembers()
	cs.updatePartitionTable()
}

func (cs *clusterService) updatePartitionTable() {
	//cs.client.PartitionService.refresh <- struct{}{}
}

func (cs *clusterService) handleMemberList(members []*proto.Member) {
	previousMembers := cs.members.Load().([]*proto.Member)
	cs.handleMemberListRemovals(previousMembers, members)
	cs.handleMemberListAdditions(previousMembers, members)
	cs.updatePartitionTable()
	cs.initialMemberListWg.Done()
}

func (cs *clusterService) handleMemberListRemovals(previousMembers []*proto.Member, members []*proto.Member) {
	for _, member := range previousMembers {
		found := false
		for _, newMember := range members {
			if member.HasSameAddress(newMember) {
				found = true
				break
			}
		}
		if !found {
			cs.memberRemoved(member)
		}
	}
}

func (cs *clusterService) handleMemberListAdditions(previousMembers []*proto.Member, members []*proto.Member) {
	for _, member := range members {
		found := false
		for _, previousMember := range previousMembers {
			if member.HasSameAddress(previousMember) {
				found = true
				break
			}
		}
		if !found {
			cs.memberAdded(member)
		}
	}
}

func (cs *clusterService) handleMemberAttributeChange(uuid string, key string, operationType int32, value string) {
	cs.notifyListenersForMemberAttributeChange(uuid, key, operationType, value)
}

func (cs *clusterService) memberAdded(member *proto.Member) {
	cs.addMemberToMembers(member)
	cs.notifyListenersForNewMember(member)
}

func (cs *clusterService) memberRemoved(member *proto.Member) {
	cs.removeMemberFromMembers(member)
	cs.closeRemovedMembersConnection(member)
	cs.notifyListenersForMemberRemoval(member)
}

func (cs *clusterService) closeRemovedMembersConnection(member *proto.Member) {
	connection := cs.client.ConnectionManager.getActiveConnection(member.Address().(*proto.Address))
	if connection != nil {
		connection.close(core.NewHazelcastTargetDisconnectedError("the client"+
			"has closed the Connection to this member after receiving a member left event from the cluster", nil))
	}
}

func (cs *clusterService) notifyListenersForMemberAttributeChange(uuid string, key string,
	operationType int32, value string) {
	rangeFunc := func(id, listener interface{}) bool {
		if _, ok := listener.(core.MemberAttributeChangedListener); ok {
			member := cs.GetMemberByUUID(uuid)
			event := proto.NewMemberAttributeEvent(operationType, key, value, member)
			listener.(core.MemberAttributeChangedListener).MemberAttributeChanged(event)
		}
		return true
	}
	cs.listeners.Range(rangeFunc)
}

func (cs *clusterService) notifyListenersForMemberRemoval(member *proto.Member) {
	rangeFunc := func(id, listener interface{}) bool {
		if _, ok := listener.(core.MemberRemovedListener); ok {
			listener.(core.MemberRemovedListener).MemberRemoved(member)
		}
		return true
	}
	cs.listeners.Range(rangeFunc)
}

func (cs *clusterService) notifyListenersForNewMember(member *proto.Member) {
	rangeFunc := func(id, listener interface{}) bool {
		if _, ok := listener.(core.MemberAddedListener); ok {
			listener.(core.MemberAddedListener).MemberAdded(member)
		}
		return true
	}
	cs.listeners.Range(rangeFunc)
}

func (cs *clusterService) removeMemberFromMembers(member *proto.Member) {
	members := cs.members.Load().([]*proto.Member)
	copyMembers := make([]*proto.Member, 0, len(members)-1)
	for _, curMember := range members {
		if !curMember.Equal(*member) {
			copyMembers = append(copyMembers, curMember)
		}
	}
	cs.members.Store(copyMembers)
}

func (cs *clusterService) addMemberToMembers(member *proto.Member) {
	members := cs.members.Load().([]*proto.Member)
	copyMembers := make([]*proto.Member, len(members))
	copy(copyMembers, members)
	copyMembers = append(copyMembers, member)
	cs.members.Store(copyMembers)
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
		if member.UUID().ToString() == uuid {
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
	if cs.shouldReconnect(connection) {
		cs.logger.Debug("Owner connection: ", cs.getOwnerConnectionAddress(), " is closed, client will try to reconnect.")
		cs.client.lifecycleService.fireLifecycleEvent(core.LifecycleStateDisconnected)
		cs.clearOwnerConnectionAddress()
		cs.reconnectChan <- struct{}{}
	}
}

func (cs *clusterService) onConnectionOpened(connection *Connection) {

}

func (cs *clusterService) shouldReconnect(connection *Connection) bool {
	address, ok := connection.endpoint.Load().(*proto.Address)
	ownerConnectionAddress := cs.getOwnerConnectionAddress()
	return ok && ownerConnectionAddress != nil &&
		*address == *ownerConnectionAddress && cs.client.lifecycleService.isLive.Load().(bool)

}

func (cs *clusterService) clearOwnerConnectionAddress() {
	cs.ownerConnectionAddress.Store(&proto.Address{})
}

func (cs *clusterService) shutdown() {
	cs.cancelChan <- struct{}{}
}

type MemberListSnapshot interface {
	GetVersion() int32
	GetMembers() map[core.UUID]core.Member
	GetMemberList() []core.Member
}

type memberListSnapshot struct {
	version int32
	members map[core.UUID]core.Member
}

func NewMemberListSnapshot(version int32, members map[core.UUID]core.Member) MemberListSnapshot {
	return memberListSnapshot{version, members}
}

func NewEmptyMemberListSnapshot() MemberListSnapshot {
	return memberListSnapshot{-1, make(map[core.UUID]core.Member, 0)}
}

func (m memberListSnapshot) GetVersion() int32 {
	return m.version
}

func (m memberListSnapshot) GetMembers() map[core.UUID]core.Member {
	return m.members
}

func (m memberListSnapshot) GetMemberList() []core.Member {
	members := make([]core.Member, 0)
	for _, eachMember := range m.members {
		members = append(members, eachMember)
	}
	return members
}

type membershipEvent struct {
	// Removed or added member.
	member core.Member

	// Members list at the moment after this event.
	members []core.Member

	// eventType is MemberEvent
	eventType core.MemberEvent
}

func NewMembershipEvent(member core.Member, members []core.Member, eventType core.MemberEvent) core.MembershipEvent {
	return membershipEvent{member: member, members: members, eventType: eventType}
}

func (m membershipEvent) GetMember() core.Member {
	return m.member
}

func (m membershipEvent) GetMembers() []core.Member {
	return m.members
}

func (m membershipEvent) GetEventType() core.MemberEvent {
	return m.eventType
}

// An event that is sent when a initialMembershipListener registers itself on a cluster.
type initialMembershipEvent struct {
	members []core.Member
}

func NewInitialMembershipEvent(members []core.Member) core.InitialMembershipEvent {
	return initialMembershipEvent{members: members}
}

func (i initialMembershipEvent) GetMembers() []core.Member {
	return i.members
}
