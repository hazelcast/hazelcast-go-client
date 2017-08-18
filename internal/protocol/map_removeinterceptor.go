// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package protocol

type MapRemoveInterceptorResponseParameters struct {
	Response bool
}

func MapRemoveInterceptorCalculateSize(name *string, id *string) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += StringCalculateSize(name)
	dataSize += StringCalculateSize(id)
	return dataSize
}

func MapRemoveInterceptorEncodeRequest(name *string, id *string) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, MapRemoveInterceptorCalculateSize(name, id))
	clientMessage.SetMessageType(MAP_REMOVEINTERCEPTOR)
	clientMessage.IsRetryable = false
	clientMessage.AppendString(name)
	clientMessage.AppendString(id)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func MapRemoveInterceptorDecodeResponse(clientMessage *ClientMessage) *MapRemoveInterceptorResponseParameters {
	// Decode response from client message
	parameters := new(MapRemoveInterceptorResponseParameters)
	parameters.Response = clientMessage.ReadBool()
	return parameters
}
