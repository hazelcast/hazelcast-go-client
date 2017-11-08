package internal

import (
	"github.com/hazelcast/go-client/config"
	"github.com/hazelcast/go-client/core"
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DEFAULT_ADDRESS       = "localhost"
	DEFAULT_PORT          = 5701
	MEMBER_ADDED    int32 = 1
	MEMBER_REMOVED  int32 = 2
)

var wg sync.WaitGroup

type ClusterService struct {
	client                 *HazelcastClient
	config                 *config.ClientConfig
	members                atomic.Value
	ownerUuid              string
	uuid                   string
	ownerConnectionAddress *Address
	listeners              atomic.Value
	mu                     sync.Mutex
	reconnectChan          chan struct{}
}

func NewClusterService(client *HazelcastClient, config *config.ClientConfig) *ClusterService {
	service := &ClusterService{client: client, config: config, reconnectChan: make(chan struct{}, 1)}
	service.members.Store(make([]Member, 0))              //Initialize
	service.listeners.Store(make(map[string]interface{})) //Initialize
	for _, membershipListener := range client.ClientConfig.MembershipListeners {
		service.AddListener(membershipListener)
	}
	service.client.ConnectionManager.AddListener(service)
	go service.process()
	return service
}
func (clusterService *ClusterService) start() error {
	return clusterService.connectToCluster()
}
func getPossibleAddresses(addressList *[]string, memberList *[]Member) *[]Address {
	if addressList == nil {
		addressList = new([]string)
	}
	if memberList == nil {
		memberList = new([]Member)
	}
	allAddresses := make(map[Address]struct{}, len(*addressList)+len(*memberList))
	for _, address := range *addressList {
		ip, port := common.GetIpAndPort(address)
		if common.IsValidIpAddress(ip) {
			allAddresses[*NewAddressWithParameters(ip, port)] = struct{}{}
		}
	}
	for _, member := range *memberList {
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
	return &addresses
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
	attempLimit := clusterService.config.ClientNetworkConfig.ConnectionAttemptLimit
	retryDelay := clusterService.config.ClientNetworkConfig.ConnectionAttemptPeriod
	for currentAttempt <= attempLimit {
		currentAttempt++
		members := clusterService.members.Load().([]Member)
		addresses := getPossibleAddresses(&clusterService.config.ClientNetworkConfig.Addresses, &members)
		for _, address := range *addresses {
			err := clusterService.connectToAddress(&address)
			if err != nil {
				log.Println("the following error occured while trying to connect to cluster: ", err)
				if _, ok := err.(*common.HazelcastAuthenticationError); ok {
					return err
				}
				continue
			}
			return nil
		}
		if currentAttempt < attempLimit {
			time.Sleep(time.Duration(retryDelay) * time.Second)
		}
	}
	return common.NewHazelcastIllegalStateError("could not connect to any addresses", nil)
}
func (clusterService *ClusterService) connectToAddress(address *Address) error {
	connectionChannel, errChannel := clusterService.client.ConnectionManager.GetOrConnect(address)
	var con *Connection
	select {
	case con = <-connectionChannel:
	case err := <-errChannel:
		return err
	}
	if !con.isOwnerConnection {
		err := clusterService.client.ConnectionManager.clusterAuthenticator(con)
		if err != nil {
			return err
		}
	}

	clusterService.ownerConnectionAddress = con.endpoint
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
	invocation := NewInvocation(request, -1, nil, connection)
	invocation.eventHandler = eventHandler
	response, err := clusterService.client.InvocationService.SendInvocation(invocation).Result()
	if err != nil {
		return err
	}
	registrationId := ClientAddMembershipListenerDecodeResponse(response).Response
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
	clusterService.client.PartitionService.refresh <- true
}

func (clusterService *ClusterService) handleMemberList(members *[]Member) {
	if len(*members) == 0 {
		return
	}
	previousMembers := clusterService.members.Load().([]Member)
	//TODO:: This loop is O(n^2), it is better to store members in a map to speed it up.
	for _, member := range previousMembers {
		found := false
		for _, newMember := range *members {
			if *member.Address().(*Address) == *newMember.Address().(*Address) {
				found = true
				break
			}
		}
		if !found {
			clusterService.memberRemoved(&member)
		}
	}
	for _, member := range *members {
		found := false
		for _, previousMember := range previousMembers {
			if *member.Address().(*Address) == *previousMember.Address().(*Address) {
				found = true
				break
			}
		}
		if !found {
			clusterService.memberAdded(&member)
		}
	}
	clusterService.client.PartitionService.refresh <- true
	wg.Done() //initial member list is fetched
}
func (clusterService *ClusterService) handleMemberAttributeChange(uuid *string, key *string, operationType int32, value *string) {
	//TODO :: implement this.
}
func (clusterService *ClusterService) memberAdded(member *Member) {
	members := clusterService.members.Load().([]Member)
	copyMembers := make([]Member, len(members))
	for index, member := range members {
		copyMembers[index] = member
	}
	copyMembers = append(copyMembers, *member)
	clusterService.members.Store(copyMembers)
	listeners := clusterService.listeners.Load().(map[string]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(MemberAddedListener); ok {
			listener.(MemberAddedListener).MemberAdded(member)
		}
	}

}
func (clusterService *ClusterService) memberRemoved(member *Member) {
	members := clusterService.members.Load().([]Member)
	copyMembers := make([]Member, len(members)-1)
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
		connection.Close(common.NewHazelcastTargetDisconnectedError("the client"+
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
	membersList := clusterService.members.Load().([]Member)
	members := make([]core.IMember, len(membersList))
	for index := 0; index < len(membersList); index++ {
		members[index] = core.IMember(&membersList[index])
	}
	return members
}
func (clusterService *ClusterService) GetMember(address *Address) *Member {
	membersList := clusterService.members.Load().([]Member)
	for _, member := range membersList {
		if member.Address() == address {
			return &member
		}
	}
	return nil
}
func (clusterService *ClusterService) onConnectionClosed(connection *Connection, cause error) {
	if connection.endpoint != nil && clusterService.ownerConnectionAddress != nil && *connection.endpoint == *clusterService.ownerConnectionAddress && clusterService.client.LifecycleService.isLive {
		clusterService.client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_DISCONNECTED)
		clusterService.ownerConnectionAddress = nil
		clusterService.reconnectChan <- struct{}{}
	}
}
func (clusterSerice *ClusterService) onConnectionOpened(connection *Connection) {

}
