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
	MAP_PUT                                = 0x0101
	MAP_GET                                = 0x0102
	MAP_REMOVE                             = 0x0103
	MAP_REPLACE                            = 0x0104
	MAP_REPLACEIFSAME                      = 0x0105
	MAP_CONTAINSKEY                        = 0x0109
	MAP_CONTAINSVALUE                      = 0x010a
	MAP_REMOVEIFSAME                       = 0x010b
	MAP_DELETE                             = 0x010c
	MAP_FLUSH                              = 0x010d
	MAP_TRYREMOVE                          = 0x010e
	MAP_TRYPUT                             = 0x010f
	MAP_PUTTRANSIENT                       = 0x0110
	MAP_PUTIFABSENT                        = 0x0111
	MAP_SET                                = 0x0112
	MAP_LOCK                               = 0x0113
	MAP_TRYLOCK                            = 0x0114
	MAP_ISLOCKED                           = 0x0115
	MAP_UNLOCK                             = 0x0116
	MAP_ADDINTERCEPTOR                     = 0x0117
	MAP_REMOVEINTERCEPTOR                  = 0x0118
	MAP_ADDENTRYLISTENERTOKEYWITHPREDICATE = 0x0119
	MAP_ADDENTRYLISTENERWITHPREDICATE      = 0x011a
	MAP_ADDENTRYLISTENERTOKEY              = 0x011b
	MAP_ADDENTRYLISTENER                   = 0x011c
	MAP_ADDNEARCACHEENTRYLISTENER          = 0x011d
	MAP_REMOVEENTRYLISTENER                = 0x011e
	MAP_ADDPARTITIONLOSTLISTENER           = 0x011f
	MAP_REMOVEPARTITIONLOSTLISTENER        = 0x0120
	MAP_GETENTRYVIEW                       = 0x0121
	MAP_EVICT                              = 0x0122
	MAP_EVICTALL                           = 0x0123
	MAP_LOADALL                            = 0x0124
	MAP_LOADGIVENKEYS                      = 0x0125
	MAP_KEYSET                             = 0x0126
	MAP_GETALL                             = 0x0127
	MAP_VALUES                             = 0x0128
	MAP_ENTRYSET                           = 0x0129
	MAP_KEYSETWITHPREDICATE                = 0x012a
	MAP_VALUESWITHPREDICATE                = 0x012b
	MAP_ENTRIESWITHPREDICATE               = 0x012c
	MAP_ADDINDEX                           = 0x012d
	MAP_SIZE                               = 0x012e
	MAP_ISEMPTY                            = 0x012f
	MAP_PUTALL                             = 0x0130
	MAP_CLEAR                              = 0x0131
	MAP_EXECUTEONKEY                       = 0x0132
	MAP_SUBMITTOKEY                        = 0x0133
	MAP_EXECUTEONALLKEYS                   = 0x0134
	MAP_EXECUTEWITHPREDICATE               = 0x0135
	MAP_EXECUTEONKEYS                      = 0x0136
	MAP_FORCEUNLOCK                        = 0x0137
	MAP_KEYSETWITHPAGINGPREDICATE          = 0x0138
	MAP_VALUESWITHPAGINGPREDICATE          = 0x0139
	MAP_ENTRIESWITHPAGINGPREDICATE         = 0x013a
	MAP_CLEARNEARCACHE                     = 0x013b
	MAP_FETCHKEYS                          = 0x013c
	MAP_FETCHENTRIES                       = 0x013d
	MAP_AGGREGATE                          = 0x013e
	MAP_AGGREGATEWITHPREDICATE             = 0x013f
	MAP_PROJECT                            = 0x0140
	MAP_PROJECTWITHPREDICATE               = 0x0141
	MAP_FETCHNEARCACHEINVALIDATIONMETADATA = 0x0142
	MAP_ASSIGNANDGETUUIDS                  = 0x0143
	MAP_REMOVEALL                          = 0x0144
	MAP_ADDNEARCACHEINVALIDATIONLISTENER   = 0x0145
	MAP_FETCHWITHQUERY                     = 0x0146
	MAP_EVENTJOURNALSUBSCRIBE              = 0x0147
	MAP_EVENTJOURNALREAD                   = 0x0148
)
