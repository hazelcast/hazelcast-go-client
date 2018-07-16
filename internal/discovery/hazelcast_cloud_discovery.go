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

package discovery

import (
	"net/http"

	"encoding/json"

	"io/ioutil"

	"strings"

	"time"

	"github.com/hazelcast/hazelcast-go-client/config/property"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/iputil"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	cloudURLPath      = "/cluster/discovery?token="
	privateAddressStr = "private-address"
	publicAddressStr  = "public-address"
)

var CloudURLBaseProperty = property.NewHazelcastPropertyString("hazelcast.client.cloud.url",
	"https://coordinator.hazelcast.cloud")

type nodeDiscoverer func() (map[string]core.Address, error)

type HazelcastCloud struct {
	endPointURL       string
	connectionTimeout time.Duration
	discoverNodes     nodeDiscoverer
}

func NewHazelcastCloud(endpointURL string, connectionTimeout time.Duration) *HazelcastCloud {
	hzCloud := &HazelcastCloud{}
	hzCloud.endPointURL = endpointURL
	hzCloud.connectionTimeout = connectionTimeout
	hzCloud.discoverNodes = hzCloud.discoverNodesInternal
	return hzCloud
}

func (hzC *HazelcastCloud) discoverNodesInternal() (map[string]core.Address, error) {
	return hzC.callService()
}

func (hzC *HazelcastCloud) callService() (map[string]core.Address, error) {
	url := hzC.endPointURL
	client := http.Client{
		Timeout: hzC.connectionTimeout,
	}
	resp, err := client.Get(url)
	// Check certificates
	if !hzC.checkCertificates(resp) {
		return nil, core.NewHazelcastCertificateError("invalid certificate from hazelcast.cloud endpoint",
			nil)
	}

	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, core.NewHazelcastIOError("got a status :"+resp.Status, nil)
	}

	return hzC.parseResponse(resp)
}

func (hzC *HazelcastCloud) checkCertificates(response *http.Response) bool {
	for _, cert := range response.TLS.PeerCertificates {
		if !cert.BasicConstraintsValid {
			return false
		}
	}
	return true
}

func (hzC *HazelcastCloud) parseResponse(response *http.Response) (map[string]core.Address, error) {
	// We could have used a struct instead of an interface that has private-address and public-address
	// fields to decode the JSON response to make things easier, but those fields names are not valid
	// because of the '-' in them.
	var target = make([]interface{}, 0)
	var privateToPublicAddrs = make(map[string]core.Address)
	resp, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(resp, &target); err != nil {
		return nil, err
	}

	// Here target is slice of maps
	for _, elem := range target {
		if mp, ok := elem.(map[string]interface{}); ok {
			var privateAddr string
			var publicAddr string
			for k, v := range mp {
				if strings.Compare(k, privateAddressStr) == 0 {
					if res, ok := v.(string); ok {
						privateAddr = res
					}
				}
				if strings.Compare(k, publicAddressStr) == 0 {
					if res, ok := v.(string); ok {
						publicAddr = res
					}
				}
			}
			publicAddress := hzC.createAddress(publicAddr)

			// TODO:: what if privateAddress is not okay ?
			// TODO:: use addressProvider
			privateAddress := proto.NewAddressWithParameters(privateAddr, int32(publicAddress.Port()))
			privateToPublicAddrs[privateAddress.String()] = publicAddress
		}
	}

	return privateToPublicAddrs, nil
}

func CreateURLEndpoint(hazelcastProperties *property.HazelcastProperties, cloudToken string) string {
	cloudBaseURL := hazelcastProperties.GetString(CloudURLBaseProperty)
	return cloudBaseURL + cloudURLPath + cloudToken
}

func (hzC *HazelcastCloud) createAddress(hostname string) core.Address {
	ip, port := iputil.GetIPAndPort(hostname)
	addr := proto.NewAddressWithParameters(ip, port)
	return addr
}
