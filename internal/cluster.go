package internal

import (
	"github.com/hazelcast/go-client/config"
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"github.com/hazelcast/go-client/core"
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
	Members                []Member
	ownerUuid              string
	uuid                   string
	ownerConnectionAddress *Address
	listeners              atomic.Value
}

func NewClusterService(client *HazelcastClient, config *config.ClientConfig) *ClusterService {
	service := &ClusterService{client: client, config: config}
	service.listeners.Store(make(map[string]interface{})) //initialize
	for _, membershipListener := range client.ClientConfig.MembershipListeners {
		service.addListener(membershipListener)
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
	addresses := getPossibleAddresses(clusterService.config.ClientNetworkConfig.Addresses, clusterService.Members)
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
func (clusterService *ClusterService) addListener(listener interface{}) *string {
	registrationId, _ := common.NewUUID()
	listeners := clusterService.listeners.Load().(map[string]interface{})
	listeners[registrationId] = listener
	return &registrationId
}
func (clusterService *ClusterService) removeListener(registrationId *string) bool {
	listeners := clusterService.listeners.Load().(map[string]interface{})
	_, found := listeners[*registrationId]
	if found {
		delete(listeners, *registrationId)
	}
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
	//TODO:: race condition for members ?
	clusterService.Members = append(clusterService.Members, *member)
	listeners := clusterService.listeners.Load().(map[string]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(MemberAddedListener); ok {
			listener.(MemberAddedListener).MemberAdded(member)
		}
	}

}
func (clusterService *ClusterService) memberRemoved(member *Member) {
	for index, cur := range clusterService.Members {
		if member.Equal(cur) {
			clusterService.Members = append(clusterService.Members[:index], clusterService.Members[index+1:]...)
			break
		}
	}
	clusterService.client.ConnectionManager.closeConnection(member.Address().(*Address))
	listeners := clusterService.listeners.Load().(map[string]interface{})
	for _, listener := range listeners {
		if _, ok := listener.(MemberRemovedListener); ok {
			listener.(MemberRemovedListener).MemberRemoved(member)
		}
	}
}
func (clusterService *ClusterService) GetMemberList() []core.IMember{
	members := make([]core.IMember,len(clusterService.Members))
	for i,m := range clusterService.Members {
		members[i] = core.IMember(&m)
	}
	return members
}
