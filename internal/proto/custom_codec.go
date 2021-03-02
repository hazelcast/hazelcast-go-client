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

import "github.com/hazelcast/hazelcast-go-client/v4/internal/core"

/*
Address Codec
*/
func AddressCodecEncode(msg *ClientMessage, address *core.Address) {
	//TODO

}

func AddressCodecDecode(msg *ClientMessage) *core.Address {
	//TODO
	return nil
}

/*
DistributedObjectInfo Codec
*/

func DistributedObjectInfoCodecDecode(msg *ClientMessage) *DistributedObjectInfo {
	//TODO
	return nil
}

/*
Member Codec
*/

func MemberCodecDecode(msg *ClientMessage) *Member {
	//TODO
	return nil
}

func DataEntryViewCodecDecode(msg *ClientMessage) *DataEntryView {
	//TODO
	return nil
}

func UUIDCodecEncode(msg *ClientMessage, uuid uuid) {

}

func UUIDCodecDecode(msg *ClientMessage) *uuid {
	//TODO
	return nil
}

/*
	Error Codec
*/

func ErrorCodecDecode(msg *ClientMessage) *ServerError {
	//TODO
	return nil

}

func DecodeStackTrace(msg *ClientMessage) core.StackTraceElement {
	//TODO
	return nil
}
