/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package azure_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/azure"
)

const (
	instance1PrivateIP = "10.240.0.2"
	instance2PrivateIP = "10.240.0.3"
	instance3PrivateIP = "10.240.0.4"
	instance1PublicIP  = "35.207.0.219"
	instance2PublicIP  = "35.237.227.147"
)

func TestExtractPrivateIPs(t *testing.T) {
	resp := makePrivateIPResponse("key1", "value1")
	j := map[string]interface{}{}
	if err := json.Unmarshal(resp, &j); err != nil {
		t.Fatal(err)
	}
	ips := azure.ExtractPrivateIPs(j)
	targetPrivateIPs := []string{instance1PrivateIP, instance2PrivateIP}
	targetPublicIPIDs := []string{
		"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip",
		"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip2",
	}
	targetTagKVs := []string{"key1", "value1", "tag2", "value2"}
	privateIPs := []string{}
	publicIPIDs := []string{}
	tagKVs := []string{}
	for _, ip := range ips {
		privateIPs = append(privateIPs, ip.PrivateIP)
		publicIPIDs = append(publicIPIDs, ip.PublicIPID)
		for k, v := range ip.Tags {
			tagKVs = append(tagKVs, k, v)
		}
	}
	assert.ElementsMatch(t, targetPrivateIPs, privateIPs)
	assert.ElementsMatch(t, targetPublicIPIDs, publicIPIDs)
	assert.ElementsMatch(t, targetTagKVs, tagKVs)
}

func TestExtractPublicIPs(t *testing.T) {
	resp := makePublicIPResponse()
	j := map[string]interface{}{}
	if err := json.Unmarshal(resp, &j); err != nil {
		t.Fatal(err)
	}
	ips := azure.ExtractPublicIPs(j)
	if !assert.Equal(t, 2, len(ips)) {
		t.FailNow()
	}
	targetKs := []string{
		"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip",
		"/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip2",
	}
	targetVs := []string{
		"35.207.0.219",
		"35.237.227.147",
	}
	ks := []string{}
	vs := []string{}
	for k, v := range ips {
		ks = append(ks, k)
		vs = append(vs, v)
	}
	assert.ElementsMatch(t, targetKs, ks)
	assert.ElementsMatch(t, targetVs, vs)
}

func TestMakeNetworkInterfaceURLFormat(t *testing.T) {
	testCases := []struct {
		fragment string
		target   string
	}{
		{fragment: "networkInterfaces", target: "%s/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/networkInterfaces?api-version=%s"},
		{fragment: "publicIPAddresses", target: "%s/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/publicIPAddresses?api-version=%s"},
	}
	for _, tc := range testCases {
		t.Run(tc.fragment, func(t *testing.T) {
			assert.Equal(t, tc.target, azure.MakeNetworkInterfaceURLFormat(tc.fragment))
		})
	}
}

func TestMakeNetworkInterfaceScaleSetURLFormat(t *testing.T) {
	testCases := []struct {
		fragment string
		target   string
	}{
		{fragment: "networkInterfaces", target: "%s/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/virtualMachineScaleSets/%s/networkInterfaces?api-version=%s"},
		{fragment: "publicIPAddresses", target: "%s/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/virtualMachineScaleSets/%s/publicIPAddresses?api-version=%s"},
	}
	for _, tc := range testCases {
		t.Run(tc.fragment, func(t *testing.T) {
			assert.Equal(t, tc.target, azure.MakeNetworkInterfaceScaleSetURLFormat(tc.fragment))
		})
	}
}

func makePrivateIPResponse(tagKey string, tagValue string) []byte {
	s := fmt.Sprintf(`{
    "value": [
        {
            "name": "test-nic",
            "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic",
            "location": "eastus",
            "properties": {
                "provisioningState": "Succeeded",
                "ipConfigurations": [
                    {
                        "name": "ipconfig1",
                        "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic/ipConfigurations/ipconfig1",
                        "properties": {
                            "provisioningState": "Succeeded",
                            "privateIPAddress": "%s",
                            "privateIPAllocationMethod": "Dynamic",
                            "publicIPAddress": {
                                "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip"
                            },
                            "subnet": {
                                "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/rg1-vnet/subnets/default"
                            },
                            "primary": true,
                            "privateIPAddressVersion": "IPv4"
                        }
                    }
                ],
                "dnsSettings": {
                    "dnsServers": [],
                    "appliedDnsServers": [],
                    "internalDomainNameSuffix": "test.bx.internal.cloudapp.net"
                },
                "macAddress": "00-0D-3A-1B-C7-21",
                "enableAcceleratedNetworking": true,
                "enableIPForwarding": false,
                "networkSecurityGroup": {
                    "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkSecurityGroups/nsg"
                },
                "primary": true,
                "virtualMachine": {
                    "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1"
                }
            },
            "tags": {
                "%s": "%s",
                "tag2": "value2"
            },
            "type": "Microsoft.Network/networkInterfaces"
        },
        {
            "name": "test-nic2",
            "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic2",
            "location": "eastus",
            "properties": {
                "provisioningState": "Succeeded",
                "ipConfigurations": [
                    {
                        "name": "ipconfig1",
                        "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic2/ipConfigurations/ipconfig1",
                        "properties": {
                            "provisioningState": "Succeeded",
                            "privateIPAddress": "%s",
                            "privateIPAllocationMethod": "Dynamic",
                            "publicIPAddress": {
                                "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip2"
                            },
                            "subnet": {
                                "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/rg1-vnet2/subnets/default"
                            },
                            "primary": true,
                            "privateIPAddressVersion": "IPv4"
                        }
                    }
                ],
                "dnsSettings": {
                    "dnsServers": [],
                    "appliedDnsServers": [],
                    "internalDomainNameSuffix": "test2.bx.internal.cloudapp.net"
                },
                "macAddress": "00-0D-3A-1B-C7-22",
                "enableAcceleratedNetworking": true,
                "enableIPForwarding": false,
                "networkSecurityGroup": {
                    "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkSecurityGroups/nsg"
                },
                "primary": true,
                "virtualMachine": {
                    "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm2"
                }
            },
            "type": "Microsoft.Network/networkInterfaces"
        },
        {
            "name": "test-nic3",
            "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic3",
            "location": "eastus",
            "properties": {
                "provisioningState": "Succeeded",
                "ipConfigurations": [
                    {
                        "name": "ipconfig1",
                        "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkInterfaces/test-nic3/ipConfigurations/ipconfig1",
                        "properties": {
                            "provisioningState": "Succeeded",
                            "privateIPAddress": "%s",
                            "privateIPAllocationMethod": "Dynamic",
                            "subnet": {
                                "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/rg1-vnet2/subnets/default"
                            },
                            "primary": true,
                            "privateIPAddressVersion": "IPv4"
                        }
                    }
                ],
                "dnsSettings": {
                    "dnsServers": [],
                    "appliedDnsServers": [],
                    "internalDomainNameSuffix": "test3.bx.internal.cloudapp.net"
                },
                "macAddress": "00-0D-3A-1B-C7-23",
                "enableAcceleratedNetworking": true,
                "enableIPForwarding": false,
                "networkSecurityGroup": {
                    "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/networkSecurityGroups/nsg"
                },
                "primary": true,
                "virtualMachine": {
                    "id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm2"
                }
            },
            "type": "Microsoft.Network/networkInterfaces"
        }
    ]
}`, instance1PrivateIP, tagKey, tagValue, instance2PrivateIP, instance3PrivateIP)
	return []byte(s)
}

func makePublicIPResponse() []byte {
	s := fmt.Sprintf(`{
		"value": [
			{
				"name": "ip02",
				"id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip",
				"location": "westus",
				"properties": {
					"provisioningState": "Succeeded",
					"ipAddress": "%s",
					"publicIPAddressVersion": "IPv4",
					"publicIPAllocationMethod": "Dynamic",
					"idleTimeoutInMinutes": 4,
					"dnsSettings": {
						"domainNameLabel": "testlbl1",
						"fqdn": "testlbl1.westus.cloudapp.azure.com"
					},
					"ipConfiguration": {
						"id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/loadBalancers/testLb/frontendIPConfigurations/LoadBalancerFrontEnd"
					}
				},
				"type": "Microsoft.Network/publicIPAddresses"
			},
			{
				"name": "ip03",
				"id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/publicIPAddresses/test-ip2",
				"location": "westus",
				"properties": {
					"provisioningState": "Succeeded",
					"ipAddress": "%s",
					"publicIPAddressVersion": "IPv4",
					"publicIPAllocationMethod": "Dynamic",
					"idleTimeoutInMinutes": 4,
					"dnsSettings": {
						"domainNameLabel": "testlbl2",
						"fqdn": "testlbl2.westus.cloudapp.azure.com"
					},
					"ipConfiguration": {
						"id": "/subscriptions/subid/resourceGroups/rg1/providers/Microsoft.Network/loadBalancers/testLb/frontendIPConfigurations/LoadBalancerFrontEnd"
					}
				},
				"type": "Microsoft.Network/publicIPAddresses"
			}
		]
	}`, instance1PublicIP, instance2PublicIP)
	return []byte(s)
}
