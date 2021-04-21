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
	"github.com/hazelcast/hazelcast-go-client/serialization"

	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

func multimapForceUnlockCalculateSize(name string, key serialization.Data, referenceId int64) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += dataCalculateSize(key)
	dataSize += bufutil.Int64SizeInBytes
	return dataSize
}

// MultiMapForceUnlockEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func MultiMapForceUnlockEncodeRequest(name string, key serialization.Data, referenceId int64) *ClientMessage {
	// Encode request into clientMessage
	//TODO
	return nil
}

// MultiMapForceUnlockDecodeResponse(clientMessage *ClientMessage), this message has no parameters to decode
