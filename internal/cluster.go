package internal

import (
	"github.com/hazelcast/go-client/config"
	"github.com/hazelcast/go-client/internal/common"
	. "github.com/hazelcast/go-client/internal/protocol"
	"log"
	"time"
)

const (
	DEFAULT_ADDRESS = "127.0.0.1"
	DEFAULT_PORT    = 5701
)

type ClusterService struct {
	client                 *HazelcastClient
	config                 *config.ClientConfig
	Members                []Member
	ownerUuid              string
	uuid                   string
	ownerConnectionAddress *Address
}

func NewClusterService(client *HazelcastClient, config *config.ClientConfig) *ClusterService {
	return &ClusterService{client: client, config: config}
}
func (clusterService *ClusterService) start() {
	clusterService.connectToCluster()
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
func (clusterService *ClusterService) connectToCluster() {
	addresses := getPossibleAddresses(&clusterService.config.ClientNetworkConfig.Addresses, &clusterService.Members)
	log.Println(*addresses)
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
	clusterService.client.LifecycleService.fireLifecycleEvent(LIFECYCLE_STATE_CONNECTED)
	return nil
}
