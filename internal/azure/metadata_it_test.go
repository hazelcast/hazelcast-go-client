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
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hazelcast/hazelcast-go-client/internal/azure"
)

type handler struct {
	doneCh chan struct{}
}

func newHandler() *handler {
	return &handler{
		doneCh: make(chan struct{}, 1),
	}
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, err := w.Write([]byte(metadataComputeText)); err != nil {
		panic(err)
	}
	h.doneCh <- struct{}{}
}

func TestMetadataAPI_FetchMetadata(t *testing.T) {
	handler := newHandler()
	server := &http.Server{
		Addr:    ":10000",
		Handler: handler,
	}
	go func() {
		server.ListenAndServe()
	}()
	time.Sleep(1 * time.Second)
	metadataAPI := azure.NewMetadataAPIWithEndpoint("http://localhost:10000")
	if err := metadataAPI.FetchMetadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "xxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx", metadataAPI.SubscriptionID())
	<-handler.doneCh
	server.Shutdown(context.Background())
}

const metadataComputeText = `
{
	"azEnvironment": "AZUREPUBLICCLOUD",
	"isHostCompatibilityLayerVm": "true",
	"licenseType":  "",
	"location": "westus",
	"name": "examplevmname",
	"offer": "UbuntuServer",
	"osProfile": {
		"adminUsername": "admin",
		"computerName": "examplevmname",
		"disablePasswordAuthentication": "true"
	},
	"osType": "Linux",
	"placementGroupId": "f67c14ab-e92c-408c-ae2d-da15866ec79a",
	"plan": {
		"name": "planName",
		"product": "planProduct",
		"publisher": "planPublisher"
	},
	"platformFaultDomain": "36",
	"platformUpdateDomain": "42",
	"publicKeys": [{
			"keyData": "ssh-rsa 0",
			"path": "/home/user/.ssh/authorized_keys0"
		},
		{
			"keyData": "ssh-rsa 1",
			"path": "/home/user/.ssh/authorized_keys1"
		}
	],
	"publisher": "Canonical",
	"resourceGroupName": "macikgo-test-may-23",
	"resourceId": "/subscriptions/xxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx/resourceGroups/macikgo-test-may-23/providers/Microsoft.Compute/virtualMachines/examplevmname",
	"securityProfile": {
		"secureBootEnabled": "true",
		"virtualTpmEnabled": "false"
	},
	"sku": "18.04-LTS",
	"storageProfile": {
		"dataDisks": [{
			"caching": "None",
			"createOption": "Empty",
			"diskSizeGB": "1024",
			"image": {
				"uri": ""
			},
			"lun": "0",
			"managedDisk": {
				"id": "/subscriptions/xxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx/resourceGroups/macikgo-test-may-23/providers/Microsoft.Compute/disks/exampledatadiskname",
				"storageAccountType": "Standard_LRS"
			},
			"name": "exampledatadiskname",
			"vhd": {
				"uri": ""
			},
			"writeAcceleratorEnabled": "false"
		}],
		"imageReference": {
			"id": "",
			"offer": "UbuntuServer",
			"publisher": "Canonical",
			"sku": "16.04.0-LTS",
			"version": "latest"
		},
		"osDisk": {
			"caching": "ReadWrite",
			"createOption": "FromImage",
			"diskSizeGB": "30",
			"diffDiskSettings": {
				"option": "Local"
			},
			"encryptionSettings": {
				"enabled": "false"
			},
			"image": {
				"uri": ""
			},
			"managedDisk": {
				"id": "/subscriptions/xxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx/resourceGroups/macikgo-test-may-23/providers/Microsoft.Compute/disks/exampleosdiskname",
				"storageAccountType": "Standard_LRS"
			},
			"name": "exampleosdiskname",
			"osType": "Linux",
			"vhd": {
				"uri": ""
			},
			"writeAcceleratorEnabled": "false"
		}
	},
	"subscriptionId": "xxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx",
	"tags": "baz:bash;foo:bar",
	"version": "15.05.22",
	"vmId": "02aab8a4-74ef-476e-8182-f6d2ba4166a6",
	"vmScaleSetName": "crpteste9vflji9",
	"vmSize": "Standard_A3",
	"zone": ""
}
`
