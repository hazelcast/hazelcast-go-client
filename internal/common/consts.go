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

package common

const (
	SERVICE_NAME_ATOMIC_LONG                     = "hz:impl:atomicLongService"
	SERVICE_NAME_ATOMIC_REFERENCE                = "hz:impl:atomicReferenceService"
	SERVICE_NAME_COUNT_DOWN_LATCH                = "hz:impl:countDownLatchService"
	SERVICE_NAME_ID_GENERATOR                    = "hz:impl:idGeneratorService"
	SERVICE_NAME_EXECUTOR                        = "hz:impl:executorService"
	SERVICE_NAME_LOCK                            = "hz:impl:lockService"
	SERVICE_NAME_LIST                            = "hz:impl:listService"
	SERVICE_NAME_MULTI_MAP                       = "hz:impl:multiMapService"
	SERVICE_NAME_MAP                             = "hz:impl:mapService"
	SERVICE_NAME_RELIABLE_TOPIC                  = "hz:impl:reliableTopicService"
	SERVICE_NAME_REPLICATED_MAP                  = "hz:impl:replicatedMapService"
	SERVICE_NAME_RINGBUFFER_SERIVCE              = "hz:impl:ringbufferService"
	SERVICE_NAME_SEMAPHORE                       = "hz:impl:semaphoreService"
	SERVICE_NAME_SET                             = "hz:impl:setService"
	SERVICE_NAME_QUEUE                           = "hz:impl:queueService"
	SERVICE_NAME_TOPIC                           = "hz:impl:topicService"
	SERVICE_NAME_ID_GENERATOR_ATOMIC_LONG_PREFIX = "hz:atomic:idGenerator:"
)

type MessageType uint16

//MESSAGE TYPES
const (
	MESSAGE_TYPE_EXCEPTION MessageType = 109
)

type ErrorCode int32

//ERROR CODES
const (
	ERROR_CODE_UNDEFINED                      ErrorCode = 0
	ERROR_CODE_ARRAY_INDEX_OUT_OF_BOUNDS      ErrorCode = 1
	ERROR_CODE_ARRAY_STORE                    ErrorCode = 2
	ERROR_CODE_AUTHENTICATION                 ErrorCode = 3
	ERROR_CODE_CACHE                          ErrorCode = 4
	ERROR_CODE_CACHE_LOADER                   ErrorCode = 5
	ERROR_CODE_CACHE_NOT_EXISTS               ErrorCode = 6
	ERROR_CODE_CACHE_WRITER                   ErrorCode = 7
	ERROR_CODE_CALLER_NOT_MEMBER              ErrorCode = 8
	ERROR_CODE_CANCELLATION                   ErrorCode = 9
	ERROR_CODE_CLASS_CAST                     ErrorCode = 10
	ERROR_CODE_CLASS_NOT_FOUND                ErrorCode = 11
	ERROR_CODE_CONCURRENT_MODIFICATION        ErrorCode = 12
	ERROR_CODE_CONFIG_MISMATCH                ErrorCode = 13
	ERROR_CODE_CONFIGURATION                  ErrorCode = 14
	ERROR_CODE_DISTRIBUTED_OBJECT_DESTROYED   ErrorCode = 15
	ERROR_CODE_DUPLICATE_INSTANCE_NAME        ErrorCode = 16
	ERROR_CODE_EOF                            ErrorCode = 17
	ERROR_CODE_ENTRY_PROCESSOR                ErrorCode = 18
	ERROR_CODE_EXECUTION                      ErrorCode = 19
	ERROR_CODE_HAZELCAST                      ErrorCode = 20
	ERROR_CODE_HAZELCAST_INSTANCE_NOT_ACTIVE  ErrorCode = 21
	ERROR_CODE_HAZELCAST_OVERLOAD             ErrorCode = 22
	ERROR_CODE_HAZELCAST_SERIALIZATION        ErrorCode = 23
	ERROR_CODE_IO                             ErrorCode = 24
	ERROR_CODE_ILLEGAL_ARGUMENT               ErrorCode = 25
	ERROR_CODE_ILLEGAL_ACCESS_EXCEPTION       ErrorCode = 26
	ERROR_CODE_ILLEGAL_ACCESS_ERROR           ErrorCode = 27
	ERROR_CODE_ILLEGAL_MONITOR_STATE          ErrorCode = 28
	ERROR_CODE_ILLEGAL_STATE                  ErrorCode = 29
	ERROR_CODE_ILLEGAL_THREAD_STATE           ErrorCode = 30
	ERROR_CODE_INDEX_OUT_OF_BOUNDS            ErrorCode = 31
	ERROR_CODE_INTERRUPTED                    ErrorCode = 32
	ERROR_CODE_INVALID_ADDRESS                ErrorCode = 33
	ERROR_CODE_INVALID_CONFIGURATION          ErrorCode = 34
	ERROR_CODE_MEMBER_LEFT                    ErrorCode = 35
	ERROR_CODE_NEGATIVE_ARRAY_SIZE            ErrorCode = 36
	ERROR_CODE_NO_SUCH_ELEMENT                ErrorCode = 37
	ERROR_CODE_NOT_SERIALIZABLE               ErrorCode = 38
	ERROR_CODE_NIL_POINTER                    ErrorCode = 39
	ERROR_CODE_OPERATION_TIMEOUT              ErrorCode = 40
	ERROR_CODE_PARTITION_MIGRATING            ErrorCode = 41
	ERROR_CODE_QUERY                          ErrorCode = 42
	ERROR_CODE_QUERY_RESULT_SIZE_EXCEEDED     ErrorCode = 43
	ERROR_CODE_QUORUM                         ErrorCode = 44
	ERROR_CODE_REACHED_MAX_SIZE               ErrorCode = 45
	ERROR_CODE_REJECTED_EXECUTION             ErrorCode = 46
	ERROR_CODE_REMOTE_MAP_REDUCE              ErrorCode = 47
	ERROR_CODE_RESPONSE_ALREADY_SENT          ErrorCode = 48
	ERROR_CODE_RETRYABLE_HAZELCAST            ErrorCode = 49
	ERROR_CODE_RETRYABLE_IO                   ErrorCode = 50
	ERROR_CODE_RUNTIME                        ErrorCode = 51
	ERROR_CODE_SECURITY                       ErrorCode = 52
	ERROR_CODE_SOCKET                         ErrorCode = 53
	ERROR_CODE_STALE_SEQUENCE                 ErrorCode = 54
	ERROR_CODE_TARGET_DISCONNECTED            ErrorCode = 55
	ERROR_CODE_TARGET_NOT_MEMBER              ErrorCode = 56
	ERROR_CODE_TIMEOUT                        ErrorCode = 57
	ERROR_CODE_TOPIC_OVERLOAD                 ErrorCode = 58
	ERROR_CODE_TOPOLOGY_CHANGED               ErrorCode = 59
	ERROR_CODE_TRANSACTION                    ErrorCode = 60
	ERROR_CODE_TRANSACTION_NOT_ACTIVE         ErrorCode = 61
	ERROR_CODE_TRANSACTION_TIMED_OUT          ErrorCode = 62
	ERROR_CODE_URI_SYNTAX                     ErrorCode = 63
	ERROR_CODE_UTF_DATA_FORMAT                ErrorCode = 64
	ERROR_CODE_UNSUPPORTED_OPERATION          ErrorCode = 65
	ERROR_CODE_WRONG_TARGET                   ErrorCode = 66
	ERROR_CODE_XA                             ErrorCode = 67
	ERROR_CODE_ACCESS_CONTROL                 ErrorCode = 68
	ERROR_CODE_LOGIN                          ErrorCode = 69
	ERROR_CODE_UNSUPPORTED_CALLBACK           ErrorCode = 70
	ERROR_CODE_NO_DATA_MEMBER                 ErrorCode = 71
	ERROR_CODE_REPLICATED_MAP_CANT_BE_CREATED ErrorCode = 72
	ERROR_CODE_MAX_MESSAGE_SIZE_EXCEEDED      ErrorCode = 73
	ERROR_CODE_WAN_REPLICATION_QUEUE_FULL     ErrorCode = 74
	ERROR_CODE_ASSERTION_ERROR                ErrorCode = 75
	ERROR_CODE_OUT_OF_MEMORY_ERROR            ErrorCode = 76
	ERROR_CODE_STACK_OVERFLOW_ERROR           ErrorCode = 77
	ERROR_CODE_NATIVE_OUT_OF_MEMORY_ERROR     ErrorCode = 78
	ERROR_CODE_NOT_FOUND                      ErrorCode = 79
	ERROR_CODE_STALE_TASK_ID                  ErrorCode = 80
	ERROR_CODE_DUPLICATE_TASK                 ErrorCode = 81
	ERROR_CODE_STALE_TASK                     ErrorCode = 82
)

/*
Event Response Constants
*/
const (
	EVENT_MEMBER                 = 200
	EVENT_MEMBERLIST             = 201
	EVENT_MEMBERATTRIBUTECHANGE  = 202
	EVENT_ENTRY                  = 203
	EVENT_ITEM                   = 204
	EVENT_TOPIC                  = 205
	EVENT_PARTITIONLOST          = 206
	EVENT_DISTRIBUTEDOBJECT      = 207
	EVENT_CACHEINVALIDATION      = 208
	EVENT_MAPPARTITIONLOST       = 209
	EVENT_CACHE                  = 210
	EVENT_CACHEBATCHINVALIDATION = 211
	// ENTERPRISE
	EVENT_QUERYCACHESINGLE = 212
	EVENT_QUERYCACHEBATCH  = 213

	EVENT_CACHEPARTITIONLOST    = 214
	EVENT_IMAPINVALIDATION      = 215
	EVENT_IMAPBATCHINVALIDATION = 216
)

const (
	BYTE_SIZE_IN_BYTES   = 1
	BOOL_SIZE_IN_BYTES   = 1
	UINT8_SIZE_IN_BYTES  = 1
	SHORT_SIZE_IN_BYTES  = 2
	CHAR_SIZE_IN_BYTES   = 2
	INT_SIZE_IN_BYTES    = 4
	INT32_SIZE_IN_BYTES  = 4
	FLOAT_SIZE_IN_BYTES  = 4
	INT64_SIZE_IN_BYTES  = 8
	DOUBLE_SIZE_IN_BYTES = 8
	LONG_SIZE_IN_BYTES   = 8

	VERSION              = 0
	BEGIN_FLAG     uint8 = 0x80
	END_FLAG       uint8 = 0x40
	BEGIN_END_FLAG uint8 = BEGIN_FLAG | END_FLAG
	LISTENER_FLAG  uint8 = 0x01

	PAYLOAD_OFFSET = 18
	SIZE_OFFSET    = 0

	FRAME_LENGTH_FIELD_OFFSET   = 0
	VERSION_FIELD_OFFSET        = FRAME_LENGTH_FIELD_OFFSET + INT_SIZE_IN_BYTES
	FLAGS_FIELD_OFFSET          = VERSION_FIELD_OFFSET + BYTE_SIZE_IN_BYTES
	TYPE_FIELD_OFFSET           = FLAGS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES
	CORRELATION_ID_FIELD_OFFSET = TYPE_FIELD_OFFSET + SHORT_SIZE_IN_BYTES
	PARTITION_ID_FIELD_OFFSET   = CORRELATION_ID_FIELD_OFFSET + INT64_SIZE_IN_BYTES
	DATA_OFFSET_FIELD_OFFSET    = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES
	HEADER_SIZE                 = DATA_OFFSET_FIELD_OFFSET + SHORT_SIZE_IN_BYTES

	NIL_ARRAY_LENGTH = -1
)

const (
	ENTRYEVENT_ADDED        int32 = 1
	ENTRYEVENT_REMOVED      int32 = 2
	ENTRYEVENT_UPDATED      int32 = 4
	ENTRYEVENT_EVICTED      int32 = 8
	ENTRYEVENT_EVICT_ALL    int32 = 16
	ENTRYEVENT_CLEAR_ALL    int32 = 32
	ENTRYEVENT_MERGED       int32 = 64
	ENTRYEVENT_EXPIRED      int32 = 128
	ENTRYEVENT_INVALIDATION int32 = 256
)
const (
	NIL_KEY_IS_NOT_ALLOWED        string = "nil key is not allowed"
	NIL_KEYS_ARE_NOT_ALLOWED      string = "nil keys collection is not allowed"
	NIL_VALUE_IS_NOT_ALLOWED      string = "nil value is not allowed"
	NIL_PREDICATE_IS_NOT_ALLOWED  string = "predicate should not be nil"
	NIL_MAP_IS_NOT_ALLOWED        string = "nil map is not allowed"
	NIL_SLICE_IS_NOT_ALLOWED      string = "nil slice is not alloweed"
	NIL_LISTENER_IS_NOT_ALLOWED   string = "nil listener is not allowed"
	NIL_AGGREGATOR_IS_NOT_ALLOWED string = "aggregator should not be nil"
	NIL_PROJECTION_IS_NOT_ALLOWED string = "projection should not be nil"
)
