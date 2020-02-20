// Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
	"crypto/x509"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	privateAddr1 = "10.47.0.8"
	privateAddr2 = "10.47.0.9"
	privateAddr3 = "10.47.0.10"
	port         = "32298"
	privatePort	 = "31115"
)

const serverResponse = "[" +
	"{\"private-address\":\"" + privateAddr1 + "\",\"public-address\":\"54.213.63.142:" + port + "\"}," +
	"{\"private-address\":\"" + privateAddr2 + "\",\"public-address\":\"54.245.77.185:" + port + "\"}," +
	"{\"private-address\":\"" + privateAddr3 + "\",\"public-address\":\"54.186.232.37:" + port + "\"}" +
	"]"

const serverPrivateLinkResponse = "[" +
	"{\"private-address\":\"" + privateAddr1 + ":" + privatePort + "\",\"public-address\":\"54.213.63.142:" + port + "\"}," +
	"{\"private-address\":\"" + privateAddr2 + ":" + privatePort + "\",\"public-address\":\"54.245.77.185:" + port + "\"}," +
	"{\"private-address\":\"" + privateAddr3 + ":" + privatePort + "\",\"public-address\":\"54.186.232.37:" + port + "\"}" +
	"]"

func CheckHazelcastResponse(t *testing.T,response string,port string) {
	// Start a local HTTP server
	server := httptest.NewTLSServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send response to be tested
		rw.Write([]byte(response))
	}))

	// Close the server when test finishes
	defer server.Close()

	cert, err := x509.ParseCertificate(server.TLS.Certificates[0].Certificate[0])
	if err != nil {
		log.Fatal(err)
	}

	certpool := x509.NewCertPool()
	certpool.AddCert(cert)

	hzC := NewHazelcastCloud(server.URL , 0, certpool)
	addresses, err := hzC.discoverNodesInternal()
	if err != nil {
		t.Fatal(err)
	}
	expPrivAddr1 := privateAddr1 + ":" + port
	expPrivAddr2 := privateAddr2 + ":" + port
	expPrivAddr3 := privateAddr3 + ":" + port

	if _, found := addresses[expPrivAddr1]; !found {
		t.Errorf("Expected %s to be in addresses", expPrivAddr1)
	}
	if _, found := addresses[expPrivAddr2]; !found {
		t.Errorf("Expected %s to be in addresses", expPrivAddr2)
	}
	if _, found := addresses[expPrivAddr3]; !found {
		t.Errorf("Expected %s to be in addresses", expPrivAddr3)
	}
}

func TestHazelcastCloudDiscoverPrivateLinkNodes(t *testing.T) {
	CheckHazelcastResponse(t, serverPrivateLinkResponse, privatePort)
}

func TestHazelcastCloudDiscoverNodes(t *testing.T) {
	CheckHazelcastResponse(t,serverResponse,port)
}

func TestHazelcastCloudDiscoveryInvalidURL(t *testing.T) {
	hzC := NewHazelcastCloud("invalid", 0, nil)
	_, err := hzC.discoverNodesInternal()
	require.Errorf(t, err, "invalid url should return an error")

}

func TestHazelcastCloudDiscoveryWithoutCertPool(t *testing.T) {
	// Start a local HTTP server
	server := httptest.NewTLSServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send response to be tested
		rw.Write([]byte(serverResponse))
	}))

	// Close the server when test finishes
	defer server.Close()

	hzC := NewHazelcastCloud(server.URL, 0, nil)
	_, err := hzC.discoverNodesInternal()
	require.Errorf(t, err, "Cloud Discovery without certPool should not connect")
}

func TestHazelcastCloudDiscoverNodesInvalidJSON(t *testing.T) {
	// Start a local HTTP server
	server := httptest.NewTLSServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send response to be tested
		rw.Write([]byte("invalidJSON"))
	}))

	// Close the server when test finishes
	defer server.Close()

	cert, err := x509.ParseCertificate(server.TLS.Certificates[0].Certificate[0])
	if err != nil {
		log.Fatal(err)
	}

	certpool := x509.NewCertPool()
	certpool.AddCert(cert)

	hzC := NewHazelcastCloud(server.URL, 0, certpool)
	_, err = hzC.discoverNodesInternal()
	require.Errorf(t, err, "non JSON response should return an error")
}

func TestHazelcastCloudDiscoverNodesWrongStatusCode(t *testing.T) {
	// Start a local HTTP server
	server := httptest.NewTLSServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send response to be tested
		http.Error(rw, "", http.StatusBadRequest)
	}))

	// Close the server when test finishes
	defer server.Close()

	cert, err := x509.ParseCertificate(server.TLS.Certificates[0].Certificate[0])
	if err != nil {
		log.Fatal(err)
	}

	certpool := x509.NewCertPool()
	certpool.AddCert(cert)

	hzC := NewHazelcastCloud(server.URL, 0, certpool)
	_, err = hzC.discoverNodesInternal()
	require.Errorf(t, err, "HTTP status other than 200 response should return an error")
}
