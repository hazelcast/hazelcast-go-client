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

package proto

import (
	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	ihzerror "github.com/hazelcast/hazelcast-go-client/v4/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

/*
Address Codec
*/
func AddressCodecEncode(msg *ClientMessage, address pubcluster.Address) {
	//TODO

}

func AddressCodecDecode(msg *ClientMessage) pubcluster.Address {
	//TODO
	return nil
}

/*
DistributedObjectInfo Codec
*/

func DistributedObjectInfoCodecDecode(msg *ClientMessage) *internal.DistributedObjectInfo {
	//TODO
	return nil
}

/*
Member Codec
*/

func MemberCodecDecode(msg *ClientMessage) pubcluster.Member {
	//TODO
	return nil
}

func DataEntryViewCodecDecode(msg *ClientMessage) *serialization.DataEntryView {
	//TODO
	return nil
}

func UUIDCodecEncode(msg *ClientMessage, uuid internal.UUID) {

}

func UUIDCodecDecode(msg *ClientMessage) internal.UUID {
	//TODO
	return nil
}

/*
	Error Codec
*/

func ErrorCodecDecode(msg *ClientMessage) *ihzerror.ServerErrorImpl {
	//TODO
	return nil

}

func DecodeStackTrace(msg *ClientMessage) hzerror.StackTraceElement {
	//TODO
	return nil
}
