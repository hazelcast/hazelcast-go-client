// Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package protocol

const (
	CLIENT_AUTHENTICATION                  = 0x0002
	CLIENT_AUTHENTICATIONCUSTOM            = 0x0003
	CLIENT_ADDMEMBERSHIPLISTENER           = 0x0004
	CLIENT_CREATEPROXY                     = 0x0005
	CLIENT_DESTROYPROXY                    = 0x0006
	CLIENT_GETPARTITIONS                   = 0x0008
	CLIENT_REMOVEALLLISTENERS              = 0x0009
	CLIENT_ADDPARTITIONLOSTLISTENER        = 0x000a
	CLIENT_REMOVEPARTITIONLOSTLISTENER     = 0x000b
	CLIENT_GETDISTRIBUTEDOBJECTS           = 0x000c
	CLIENT_ADDDISTRIBUTEDOBJECTLISTENER    = 0x000d
	CLIENT_REMOVEDISTRIBUTEDOBJECTLISTENER = 0x000e
	CLIENT_PING                            = 0x000f
	CLIENT_STATISTICS                      = 0x0010
	CLIENT_DEPLOYCLASSES                   = 0x0011
)
