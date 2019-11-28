/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package proto

import (
"bytes"
"github.com/hazelcast/hazelcast-go-client/serialization"
_ "github.com/hazelcast/hazelcast-go-client"
)

type CacheConfigHolder struct {
name string
managerPrefix string
uriString string
backupCount int
asyncBackupCount int
inMemoryFormat string
evictionConfigHolder EvictionConfigHolder
wanReplicationRef WanReplicationRef
keyClassName string
valueClassName string
cacheLoaderFactory serialization.Data
cacheWriterFactory serialization.Data
expiryPolicyFactory serialization.Data
readThrough bool
writeThrough bool
storeByValue bool
managementEnabled bool
statisticsEnabled bool
hotRestartConfig HotRestartConfig
eventJournalConfig EventJournalConfig
splitBrainProtectionName string
listenerConfigurations []serialization.Data
mergePolicyConfig MergePolicyConfig
disablePerEntryInvalidationEvents bool
cachePartitionLostListenerConfigs []]ListenerConfigHolder
}

//@Generated("c6d08fe4947a32cbf2b7a9ae6c13548b")
const (
    CacheConfigHolderBackupCountFieldOffset = 0
    CacheConfigHolderAsyncBackupCountFieldOffset = CacheConfigHolderBackupCountFieldOffset + bufutil.IntSizeInBytes
    CacheConfigHolderReadThroughFieldOffset = CacheConfigHolderAsyncBackupCountFieldOffset + bufutil.IntSizeInBytes
    CacheConfigHolderWriteThroughFieldOffset = CacheConfigHolderReadThroughFieldOffset + bufutil.BooleanSizeInBytes
    CacheConfigHolderStoreByValueFieldOffset = CacheConfigHolderWriteThroughFieldOffset + bufutil.BooleanSizeInBytes
    CacheConfigHolderManagementEnabledFieldOffset = CacheConfigHolderStoreByValueFieldOffset + bufutil.BooleanSizeInBytes
    CacheConfigHolderStatisticsEnabledFieldOffset = CacheConfigHolderManagementEnabledFieldOffset + bufutil.BooleanSizeInBytes
    CacheConfigHolderDisablePerEntryInvalidationEventsFieldOffset = CacheConfigHolderStatisticsEnabledFieldOffset + bufutil.BooleanSizeInBytes
    CacheConfigHolderInitialFrameSize = CacheConfigHolderDisablePerEntryInvalidationEventsFieldOffset + bufutil.BooleanSizeInBytes
)

func CacheConfigHolderEncode(clientMessage bufutil.ClientMessagex, cacheConfigHolder CacheConfigHolder) {
        clientMessage.Add(bufutil.BeginFrame)
        initialFrame := bufutil.Frame{make([]byte, CacheConfigHolderInitialFrameSize), bufutil.UnfragmentedMessage}
        bufutil.EncodeInt(initialFrame.Content, CacheConfigHolderBackupCountFieldOffset, cacheConfigHolder.backupCount)
        bufutil.EncodeInt(initialFrame.Content, CacheConfigHolderAsyncBackupCountFieldOffset, cacheConfigHolder.asyncBackupCount)
        bufutil.EncodeBoolean(initialFrame.Content, CacheConfigHolderReadThroughFieldOffset, cacheConfigHolder.isreadThrough)
        bufutil.EncodeBoolean(initialFrame.Content, CacheConfigHolderWriteThroughFieldOffset, cacheConfigHolder.iswriteThrough)
        bufutil.EncodeBoolean(initialFrame.Content, CacheConfigHolderStoreByValueFieldOffset, cacheConfigHolder.isstoreByValue)
        bufutil.EncodeBoolean(initialFrame.Content, CacheConfigHolderManagementEnabledFieldOffset, cacheConfigHolder.ismanagementEnabled)
        bufutil.EncodeBoolean(initialFrame.Content, CacheConfigHolderStatisticsEnabledFieldOffset, cacheConfigHolder.isstatisticsEnabled)
        bufutil.EncodeBoolean(initialFrame.Content, CacheConfigHolderDisablePerEntryInvalidationEventsFieldOffset, cacheConfigHolder.isdisablePerEntryInvalidationEvents)
        clientMessage.Add(initialFrame)
        StringCodec.encode(clientMessage, cacheConfigHolder.name)
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.managerPrefix, String.encode)
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.uriString, String.encode)
        StringCodec.encode(clientMessage, cacheConfigHolder.inMemoryFormat)
        EvictionConfigHolderCodec.encode(clientMessage, cacheConfigHolder.evictionConfigHolder)
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.wanReplicationRef, WanReplicationRef.encode)
        StringCodec.encode(clientMessage, cacheConfigHolder.keyClassName)
        StringCodec.encode(clientMessage, cacheConfigHolder.valueClassName)
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.cacheLoaderFactory, Data.encode)
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.cacheWriterFactory, Data.encode)
        DataCodec.encode(clientMessage, cacheConfigHolder.expiryPolicyFactory)
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.hotRestartConfig, HotRestartConfig.encode)
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.eventJournalConfig, EventJournalConfig.encode)
        CodecUtil.encodeNullable(clientMessage, cacheConfigHolder.splitBrainProtectionName, String.encode)
        ListMultiFrameCodec.encodeNullable(clientMessage, cacheConfigHolder.getListenerConfigurations(), Data.encode)
        MergePolicyConfigCodec.encode(clientMessage, cacheConfigHolder.mergePolicyConfig)
        ListMultiFrameCodec.encodeNullable(clientMessage, cacheConfigHolder.getCachePartitionLostListenerConfigs(), ListenerConfigHolder.encode)

        clientMessage.Add(bufutil.EndFrame)
    }
func CacheConfigHolderDecode(iterator bufutil.ClientMessagex)  *CacheConfigHolder  {
        // begin frame
        iterator.Next()
        initialFrame := iterator.Next()
        backupCount := bufutil.DecodeInt(initialFrame.Content, CacheConfigHolderBackupCountFieldOffset)
        asyncBackupCount := bufutil.DecodeInt(initialFrame.Content, CacheConfigHolderAsyncBackupCountFieldOffset)
        readThrough := bufutil.DecodeBoolean(initialFrame.Content, CacheConfigHolderReadThroughFieldOffset)
        writeThrough := bufutil.DecodeBoolean(initialFrame.Content, CacheConfigHolderWriteThroughFieldOffset)
        storeByValue := bufutil.DecodeBoolean(initialFrame.Content, CacheConfigHolderStoreByValueFieldOffset)
        managementEnabled := bufutil.DecodeBoolean(initialFrame.Content, CacheConfigHolderManagementEnabledFieldOffset)
        statisticsEnabled := bufutil.DecodeBoolean(initialFrame.Content, CacheConfigHolderStatisticsEnabledFieldOffset)
        disablePerEntryInvalidationEvents := bufutil.DecodeBoolean(initialFrame.Content, CacheConfigHolderDisablePerEntryInvalidationEventsFieldOffset)
        name := StringCodec.decode(iterator)
        managerPrefix := CodecUtil.decodeNullable(iterator, String.decode)
        uriString := CodecUtil.decodeNullable(iterator, String.decode)
        inMemoryFormat := StringCodec.decode(iterator)
        evictionConfigHolder := EvictionConfigHolderCodec.decode(iterator)
        wanReplicationRef := CodecUtil.decodeNullable(iterator, WanReplicationRef.decode)
        keyClassName := StringCodec.decode(iterator)
        valueClassName := StringCodec.decode(iterator)
        cacheLoaderFactory := CodecUtil.decodeNullable(iterator, Data.decode)
        cacheWriterFactory := CodecUtil.decodeNullable(iterator, Data.decode)
        expiryPolicyFactory := DataCodec.decode(iterator)
        hotRestartConfig := CodecUtil.decodeNullable(iterator, HotRestartConfig.decode)
        eventJournalConfig := CodecUtil.decodeNullable(iterator, EventJournalConfig.decode)
        splitBrainProtectionName := CodecUtil.decodeNullable(iterator, String.decode)
        listenerConfigurations := ListMultiFrameCodec.decodeNullable(iterator, Data.decode)
        mergePolicyConfig := MergePolicyConfigCodec.decode(iterator)
        cachePartitionLostListenerConfigs := ListMultiFrameCodec.decodeNullable(iterator, ListenerConfigHolder.decode)

        bufutil.fastForwardToEndFrame(iterator)

        return &CacheConfigHolder { name, managerPrefix, uriString, backupCount, asyncBackupCount, inMemoryFormat, evictionConfigHolder, wanReplicationRef, keyClassName, valueClassName, cacheLoaderFactory, cacheWriterFactory, expiryPolicyFactory, readThrough, writeThrough, storeByValue, managementEnabled, statisticsEnabled, hotRestartConfig, eventJournalConfig, splitBrainProtectionName, listenerConfigurations, mergePolicyConfig, disablePerEntryInvalidationEvents, cachePartitionLostListenerConfigs }
    }