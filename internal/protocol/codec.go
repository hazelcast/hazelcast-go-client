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

package protocol

type Codec interface {
	EncodeRequest(args ...interface{}) (request *ClientMessage)
	DecodeResponse(clientMessage *ClientMessage, toObject ToObject) (parameters interface{}, err error)
}

var (
	MapPutCodec                                = &mapPutCodec{}
	MapTryPutCodec                             = &mapTryPutCodec{}
	MapPutTransientCodec                       = &mapPutTransientCodec{}
	MapGetCodec                                = &mapGetCodec{}
	MapRemoveCodec                             = &mapRemoveCodec{}
	MapRemoveIfSameCodec                       = &mapRemoveIfSameCodec{}
	MapRemoveAllCodec                          = &mapRemoveAllCodec{}
	MapTryRemoveCodec                          = &mapTryRemoveCodec{}
	MapSizeCodec                               = &mapSizeCodec{}
	MapAddEntryListenerCodec                   = &mapAddEntryListenerCodec{}
	MapContainsKeyCodec                        = &mapContainsKeyCodec{}
	MapContainsValueCodec                      = &mapContainsValueCodec{}
	MapClearCodec                              = &mapClearCodec{}
	MapDeleteCodec                             = &mapDeleteCodec{}
	MapIsEmptyCodec                            = &mapIsEmptyCodec{}
	MapAddIndexCodec                           = &mapAddIndexCodec{}
	MapEvictCodec                              = &mapEvictCodec{}
	MapEvictAllCodec                           = &mapEvictAllCodec{}
	MapFlushCodec                              = &mapFlushCodec{}
	MapLockCodec                               = &mapLockCodec{}
	MapTryLockCodec                            = &mapTryLockCodec{}
	MapUnlockCodec                             = &mapUnlockCodec{}
	MapForceUnlockCodec                        = &mapForceUnlockCodec{}
	MapIsLockedCodec                           = &mapIsLockedCodec{}
	MapReplaceCodec                            = &mapReplaceCodec{}
	MapReplaceIfSameCodec                      = &mapReplaceIfSameCodec{}
	MapSetCodec                                = &mapSetCodec{}
	MapPutIfAbsentCodec                        = &mapPutIfAbsentCodec{}
	MapPutAllCodec                             = &mapPutAllCodec{}
	MapKeySetCodec                             = &mapKeySetCodec{}
	MapKeySetWithPredicateCodec                = &mapKeySetWithPredicateCodec{}
	MapValuesCodec                             = &mapValuesCodec{}
	MapValuesWithPredicateCodec                = &mapValuesWithPredicateCodec{}
	MapEntrySetCodec                           = &mapEntrySetCodec{}
	MapEntriesWithPredicateCodec               = &mapEntriesWithPredicateCodec{}
	MapGetAllCodec                             = &mapGetAllCodec{}
	MapGetEntryViewCodec                       = &mapGetEntryViewCodec{}
	MapRemoveEntryListenerCodec                = &mapRemoveEntryListenerCodec{}
	MapAddEntryListenerWithPredicateCodec      = &mapAddEntryListenerWithPredicateCodec{}
	MapAddEntryListenerToKeyCodec              = &mapAddEntryListenerToKeyCodec{}
	MapAddEntryListenerToKeyWithPredicateCodec = &mapAddEntryListenerToKeyWithPredicateCodec{}
	MapExecuteOnKeyCodec                       = &mapExecuteOnKeyCodec{}
	MapExecuteWithPredicateCodec               = &mapExecuteWithPredicateCodec{}
	MapExecuteOnKeysCodec                      = &mapExecuteOnKeysCodec{}
	MapExecuteOnAllKeysCodec                   = &mapExecuteOnAllKeysCodec{}
)
