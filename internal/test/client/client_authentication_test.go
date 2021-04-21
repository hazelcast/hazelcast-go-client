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

package client

import (
	"testing"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/security"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization/spi"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var samplePortableFactoryID int32 = 666

func TestCustomAuthentication(t *testing.T) {
	shutdownFunc := testutil.CreateCluster(remoteController)
	defer shutdownFunc()

	testCases := [...]struct {
		username string
		password string
		err      bool
	}{
		{"dev", "dev-pass", false},
		{"dev", "invalidPass", false},
		{"invalidUsername", "dev-pass", true},
	}

	for _, tc := range testCases {
		cfg := hazelcast.NewConfig()
		cfg.SerializationConfig().AddPortableFactory(samplePortableFactoryID, &portableFactory{})

		cfg.SecurityConfig().SetCredentials(&CustomCredentials{
			security.NewUsernamePasswordCredentials(
				tc.username,
				tc.password,
			),
		})
		client, err := hazelcast.NewClientWithConfig(cfg)
		if tc.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
		client.Shutdown()
	}
}

func TestSerializationOfCredentials(t *testing.T) {
	cfg := serialization.NewConfig()
	creds := security.NewUsernamePasswordCredentials(
		"dev",
		"dev-pass",
	)
	cfg.AddPortableFactory(creds.FactoryID(), &portableFactory2{})
	serializationService, _ := spi.NewSerializationService(cfg)
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
