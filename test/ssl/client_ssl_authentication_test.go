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

// +build enterprise

package ssl

import (
	"log"
	"testing"

	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/config"
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/rc"
	"github.com/hazelcast/hazelcast-go-client/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var remoteController rc.RemoteController

var (
	maOptionalXML = "hazelcast-ma-optional.xml"
	maRequiredXML = "hazelcast-ma-required.xml"
	trustedCAXML  = "hazelcast-trustedCA.xml"
	letsencrypt   = "letsencrypt.jks"
	client1Cert   = "client1-cert.pem"
	client2Cert   = "client2-cert.pem"
	client1Key    = "client1-key.pem"
	client2Key    = "client2-key.pem"
	serverName    = "foo.bar.com"
	server1CA     = "server1.pem"
	server2CA     = "server2.pem"
	clientCertPw  = "cert.pem"
	clientKeyPw   = "privkey.pem"
	password      = "foobar"
)

func TestMain(m *testing.M) {
	rc, err := rc.NewRemoteControllerClient("localhost:9701")
	remoteController = rc
	if remoteController == nil || err != nil {
		log.Fatal("create remote controller failed:", err)
	}
	m.Run()
}

func createMemberWithXML(path string) (clusterID string, err error) {
	config, err := test.Read(path)
	if err != nil {
		return "", err
	}
	return createMemberWithConfig(config)
}

func createMemberWithConfig(config string) (clusterID string, err error) {
	cluster, err := remoteController.CreateCluster("", config)
	if err != nil {
		return "", err
	}
	remoteController.StartMember(cluster.ID)
	return cluster.ID, nil
}

func createClientConfigWithSSLConfig(clientCertPath string, clientKeyPath string, caPath string) (*config.Config, error) {
	config := hazelcast.NewConfig()
	sslConfig := config.NetworkConfig().SSLConfig()
	sslConfig.SetEnabled(true)
	err := sslConfig.SetCaPath(caPath)
	if err != nil {
		return nil, err
	}
	err = sslConfig.AddClientCertAndKeyPath(clientCertPath, clientKeyPath)
	if err != nil {
		return nil, err
	}
	sslConfig.ServerName = serverName
	return config, nil
}

func TestSSLMutualAuthenticationConnect(t *testing.T) {
	clusterID, err := createMemberWithXML(maRequiredXML)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)
	config, err := createClientConfigWithSSLConfig(client1Cert,
		client1Key, server1CA)
	if err != nil {
		t.Fatal(err)
	}
	client, err := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()

	if err != nil {
		t.Fatal(err)
	}
}

func TestSSLMutualAuthentication_ClientDoesntKnowServerFail(t *testing.T) {
	clusterID, err := createMemberWithXML(maRequiredXML)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)
	config, err := createClientConfigWithSSLConfig(client1Cert,
		client1Key, server2CA)
	if err != nil {
		t.Fatal(err)
	}
	_, err = hazelcast.NewClientWithConfig(config)
	if _, ok := err.(*core.HazelcastIllegalStateError); !ok {
		t.Error(err)
	}
}

func TestSSLMutualAuthentication_ServerDoesntKnowClientFail(t *testing.T) {
	clusterID, err := createMemberWithXML(maRequiredXML)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)
	config, err := createClientConfigWithSSLConfig(client2Cert,
		client2Key, server1CA)
	if err != nil {
		t.Fatal(err)
	}
	_, err = hazelcast.NewClientWithConfig(config)
	if _, ok := err.(*core.HazelcastIllegalStateError); !ok {
		t.Error(err)
	}
}

func TestSSLMutualAuthentication_NeitherServerNorClientKnowsTheOtherFail(t *testing.T) {
	clusterID, err := createMemberWithXML(maRequiredXML)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)
	config, err := createClientConfigWithSSLConfig(client2Cert,
		client2Key, server2CA)
	if err != nil {
		t.Fatal(err)
	}
	_, err = hazelcast.NewClientWithConfig(config)
	if _, ok := err.(*core.HazelcastIllegalStateError); !ok {
		t.Error(err)
	}
}

func TestSSLOptionalMutualAuthenticationConnect(t *testing.T) {
	clusterID, err := createMemberWithXML(maOptionalXML)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)
	config, err := createClientConfigWithSSLConfig(client1Cert,
		client1Key, server1CA)
	if err != nil {
		t.Fatal(err)
	}
	client, err := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()

	if err != nil {
		t.Fatal(err)
	}
}

func TestSSLOptionalMutualAuthentication_ClientDoesntKnowServerFail(t *testing.T) {
	clusterID, err := createMemberWithXML(maOptionalXML)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)
	config, err := createClientConfigWithSSLConfig(client1Cert,
		client1Key, server2CA)
	if err != nil {
		t.Fatal(err)
	}
	_, err = hazelcast.NewClientWithConfig(config)
	if _, ok := err.(*core.HazelcastIllegalStateError); !ok {
		t.Error(err)
	}
}

func TestSSLOptionalMutualAuthentication_ServerDoesntKnowClientConnect(t *testing.T) {
	clusterID, err := createMemberWithXML(maOptionalXML)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)
	config, err := createClientConfigWithSSLConfig(client2Cert,
		client2Key, server1CA)
	if err != nil {
		t.Fatal(err)
	}
	config.NetworkConfig().SSLConfig().Certificates = nil
	client, err := hazelcast.NewClientWithConfig(config)
	defer client.Shutdown()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSSLOptionalMutualAuthentication_NeitherServerNorClientKnowsTheOther(t *testing.T) {
	clusterID, err := createMemberWithXML(maOptionalXML)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)
	config, err := createClientConfigWithSSLConfig(client2Cert,
		client2Key, server2CA)
	if err != nil {
		t.Fatal(err)
	}

	_, err = hazelcast.NewClientWithConfig(config)
	if _, ok := err.(*core.HazelcastIllegalStateError); !ok {
		t.Error(err)
	}
}

func TestSSL_NoClientCertificates(t *testing.T) {
	clusterID, err := createMemberWithXML(maOptionalXML)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)
	config := hazelcast.NewConfig()
	sslConfig := config.NetworkConfig().SSLConfig()
	sslConfig.SetEnabled(true)
	err = sslConfig.SetCaPath(server1CA)
	require.NoError(t, err)
	sslConfig.ServerName = serverName
	if err != nil {
		t.Fatal(err)
	}
	_, err = hazelcast.NewClientWithConfig(config)
	assert.NoError(t, err)
}

// letsencrypt.jks common name is "member1.hazelcast-test.download".
func TestSSL_OnlyServerName(t *testing.T) {
	trustedCAXML := generateTrustedCAXML()
	clusterID, err := createMemberWithConfig(trustedCAXML)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)
	config := hazelcast.NewConfig()
	sslConfig := config.NetworkConfig().SSLConfig()
	sslConfig.SetEnabled(true)
	sslConfig.ServerName = "member1.hazelcast-test.download"
	_, err = hazelcast.NewClientWithConfig(config)
	assert.NoError(t, err)
}

func TestSSL_NoServerName(t *testing.T) {
	trustedCAXML := generateTrustedCAXML()
	clusterID, err := createMemberWithConfig(trustedCAXML)
	if err != nil {
		t.Fatal(err)
	}
	defer remoteController.ShutdownCluster(clusterID)
	config := hazelcast.NewConfig()
	sslConfig := config.NetworkConfig().SSLConfig()
	sslConfig.SetEnabled(true)
	_, err = hazelcast.NewClientWithConfig(config)
	assert.Error(t, err)
}

func generateTrustedCAXML() string {
	xml := "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" +
		"    <network>\n" +
		"        <ssl enabled=\"true\">\r\n" +
		"		 <factory-class-name>\n" +
		"			com.hazelcast.nio.ssl.ClasspathSSLContextFactory\n" +
		"		 </factory-class-name>" +
		"          <properties>\r\n" +
		"            <property name=\"keyStore\">" + "com/hazelcast/nio/ssl/letsencrypt.jks" + "</property>\r\n" +
		"            <property name=\"keyStorePassword\">123456</property>\r\n" +
		"          </properties>\r\n" +
		"        </ssl>\r\n" +
		"    </network>\n" +
		"</hazelcast>\n"
	return xml
}
