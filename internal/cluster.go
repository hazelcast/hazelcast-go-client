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
}

func NewClusterService(client *HazelcastClient, config *config.ClientConfig) *ClusterService {
	service := &ClusterService{client: client, config: config}
	service.members.Store(make([]Member, 0))              //Initialize
	service.listeners.Store(make(map[string]interface{})) //Initialize
	for _, membershipListener := range client.ClientConfig.MembershipListeners {
		service.AddListener(membershipListener)
	}
	return service
}
func (clusterService *ClusterService) start() {
	clusterService.connectToCluster()
}
func getPossibleAddresses(addressList *[]config.Address, memberList []Member) *[]Address {
	//TODO Get all possible addresses.
	addresses := make([]Address, 0)
	addresses = append(addresses, *NewAddressWithParameters(DEFAULT_ADDRESS, DEFAULT_PORT))
	return &addresses
}
func (clusterService *ClusterService) connectToCluster() {
	members := clusterService.members.Load().([]Member)
	addresses := getPossibleAddresses(clusterService.config.ClientNetworkConfig.Addresses, members)
	currentAttempt := int32(1)
	attempLimit := clusterService.config.ClientNetworkConfig.ConnectionAttemptLimit
	retryDelay := clusterService.config.ClientNetworkConfig.ConnectionAttemptPeriod
	for currentAttempt < attempLimit {
		for _, address := range *addresses {
			if currentAttempt > attempLimit {
				break
			}
			err := clusterService.connectToAddress(&address)
			if err != nil {
				//TODO :: Handle error
				currentAttempt += 1
				time.Sleep(time.Duration(retryDelay))
				continue
			}
			return
		}
	}
}
func (clusterService *ClusterService) connectToAddress(address *Address) error {
	connectionChannel := clusterService.client.ConnectionManager.GetConnection(address)
	con, alive := <-connectionChannel
	if !alive {
		log.Println("Connection is closed")
		return nil
	}
	if !con.isOwnerConnection {
		clusterService.client.ConnectionManager.clusterAuthenticator(con)
	}

	clusterService.ownerConnectionAddress = con.endpoint
	clusterService.initMembershipListener(con)
	clusterService.client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_CONNECTED)
	return nil
}
func (clusterService *ClusterService) initMembershipListener(connection *Connection) {
	wg.Add(1)
	request := ClientAddMembershipListenerEncodeRequest(false)
	eventHandler := func(message *ClientMessage) {
		ClientAddMembershipListenerHandle(message, clusterService.handleMember, clusterService.handleMemberList, clusterService.handleMemberAttributeChange)
	}
	invocation := NewInvocation(request, -1, nil, connection)
	invocation.eventHandler = eventHandler
	response, err := clusterService.client.InvocationService.SendInvocation(invocation).Result()
	if err != nil {
		//TODO:: Handle error
	}
	registrationId := ClientAddMembershipListenerDecodeResponse(response).Response
	wg.Wait() //Wait until the inital member list is fetched.
	log.Println("Registered membership listener with Id ", *registrationId)
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
	for _, member := range *members {
		clusterService.memberAdded(&member)
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
	for _, member := range members {
		if !member.Equal(member) {
			copyMembers[index] = member
			index++
		}
	}
	clusterService.members.Store(copyMembers)
	clusterService.client.ConnectionManager.closeConnection(member.Address())
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
	for i, m := range membersList {
		members[i] = core.IMember(&m)
	}
	return members
}
