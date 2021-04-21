// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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

package proto

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/core"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/bufutil"
)

type ClientAuthenticationDecodeResponseDelegate func(clientMessage *ClientMessage) (
	status uint8,
	address *core.Address,
	uuid core.UUID,
	ownerUuid core.UUID,
	serializationVersion uint8,
	serverHazelcastVersion string,
	partitionCount int32,
	clientUnregisteredMembers []*Member)

func clientAuthenticationCalculateSize(username string, password string, uuid string, ownerUuid string, isOwnerConnection bool, clientType string, serializationVersion uint8, clientHazelcastVersion string) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(username)
	dataSize += stringCalculateSize(password)
	dataSize += bufutil.BoolSizeInBytes
	if uuid != "" {
		dataSize += stringCalculateSize(uuid)
	}
	dataSize += bufutil.BoolSizeInBytes
	if ownerUuid != "" {
		dataSize += stringCalculateSize(ownerUuid)
	}
	dataSize += bufutil.BoolSizeInBytes
	dataSize += stringCalculateSize(clientType)
	dataSize += bufutil.Uint8SizeInBytes
	dataSize += stringCalculateSize(clientHazelcastVersion)
	return dataSize
}

// EncodeClientAuthenticationRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func EncodeClientAuthenticationRequest(username string, password string, uuid core.UUID, ownerUuid core.UUID, isOwnerConnection bool, clientType string, serializationVersion uint8, clientHazelcastVersion string) *ClientMessage {
	//return codec.EncodeClientAuthenticationRequest()
	return nil
}

// DecodeClientAuthenticationResponse decodes the given client message.
func DecodeClientAuthenticationResponse(clientMessage *ClientMessage) (
	status uint8,
	address *core.Address,
	uuid core.UUID,
	ownerUuid core.UUID,
	serializationVersion uint8,
	serverHazelcastVersion string,
	partitionCount int32,
	clientUnregisteredMembers []*Member) {
	// TODO:
	return
}
