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

package test

import (
	"testing"

	hazelcast "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	serialization2 "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/security"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var samplePortableFactoryID int32 = 666

func TestCustomAuthentication(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)

	cfg := hazelcast.NewConfig()
	cfg.SerializationConfig().AddPortableFactory(samplePortableFactoryID, &portableFactory{})

	cfg.SecurityConfig().SetCredentials(&CustomCredentials{
		security.NewUsernamePasswordCredentials(
			"dev",
			"dev-pass",
		),
	})

	client, _ := hazelcast.NewClientWithConfig(cfg)
	defer client.Shutdown()
	mp, _ := client.GetMap("myMap")
	_, err := mp.Put("key", "value")

	assert.NoError(t, err)
}

func TestCustomAuthenticationWithInvalidPassword(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)

	cfg := hazelcast.NewConfig()
	cfg.SerializationConfig().AddPortableFactory(samplePortableFactoryID, &portableFactory{})

	cfg.SecurityConfig().SetCredentials(&CustomCredentials{
		security.NewUsernamePasswordCredentials(
			"dev",
			"invalidPass",
		),
	})

	_, err := hazelcast.NewClientWithConfig(cfg)
	assert.NoError(t, err)
}

func TestCustomAuthenticationWithInvalidUsername(t *testing.T) {
	cluster, _ := remoteController.CreateCluster("", DefaultServerConfig)
	remoteController.StartMember(cluster.ID)
	defer remoteController.ShutdownCluster(cluster.ID)

	cfg := hazelcast.NewConfig()
	cfg.SerializationConfig().AddPortableFactory(samplePortableFactoryID, &portableFactory{})

	cfg.SecurityConfig().SetCredentials(&CustomCredentials{
		security.NewUsernamePasswordCredentials(
			"invalidUsername",
			"dev-pass",
		),
	})

	_, err := hazelcast.NewClientWithConfig(cfg)
	assert.Errorf(t, err, "Client should not connect with invalid username")
}

func TestSerializationOfCredentials(t *testing.T) {
	cfg := config.NewSerializationConfig()
	creds := security.NewUsernamePasswordCredentials(
		"dev",
		"dev-pass",
	)
	cfg.AddPortableFactory(creds.FactoryID(), &portableFactory2{})
	serializationService, _ := serialization2.NewSerializationService(cfg)
	credsData, err := serializationService.ToData(creds)
	require.NoError(t, err)
	retCreds, err := serializationService.ToObject(credsData)
	require.NoError(t, err)
	assert.Equal(t, retCreds.(*security.UsernamePasswordCredentials).Username(), "dev")
	assert.Equal(t, retCreds.(*security.UsernamePasswordCredentials).Principal(), "dev")
}

type portableFactory2 struct {
}

func (pf *portableFactory2) Create(classID int32) serialization.Portable {
	if classID == 1 {
		return &security.UsernamePasswordCredentials{
			BaseCredentials: &security.BaseCredentials{},
		}
	}
	return nil
}

type portableFactory struct {
}

func (pf *portableFactory) Create(classID int32) serialization.Portable {
	if classID == samplePortableFactoryID {
		return &CustomCredentials{}
	}
	return nil
}

type CustomCredentials struct {
	*security.UsernamePasswordCredentials
}

func (cc *CustomCredentials) FactoryID() (factoryID int32) {
	return samplePortableFactoryID
}

func (cc *CustomCredentials) ClassID() (classID int32) {
	return 7
}
