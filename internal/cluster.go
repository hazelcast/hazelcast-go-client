package internal

import (
	. "github.com/hazelcast/go-client/internal/protocol"
	"github.com/hazelcast/go-client/config"
	"time"
)

const (
	DEFAULT_ADDRESS = "127.0.0.1"
	DEFAULT_PORT = 5701
)
type ClusterService struct {
	client *HazelcastClient
	config *config.ClientConfig
	Members []Member
	ownerUuid string
	uuid string
	ownerConnectionAddress *Address
}

func NewClusterService(client *HazelcastClient,config *config.ClientConfig) *ClusterService {
	return &ClusterService{client:client,config:config}
}
func (clusterService *ClusterService) start(){
	clusterService.connectToCluster()
}
func getPossibleAddresses(addressList *[]config.Address,memberList []Member) *[]config.Address{
	//TODO Get all possible addresses.
	addresses := make([]config.Address,0)
	addresses = append(addresses,config.Address{DEFAULT_ADDRESS,DEFAULT_PORT})
	return &addresses
}
func (clusterService *ClusterService) connectToCluster(){
	addresses := getPossibleAddresses(clusterService.config.ClientNetworkConfig.Addresses,clusterService.Members)
	currentAttempt := int32(1)
	attempLimit := clusterService.config.ClientNetworkConfig.ConnectionAttemptLimit
	retryDelay := clusterService.config.ClientNetworkConfig.ConnectionAttemptPeriod
	for currentAttempt < attempLimit {
		for _,address := range *addresses {
			if currentAttempt > attempLimit{
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
func (clusterService *ClusterService) connectToAddress(address *config.Address) error{
	return nil
}