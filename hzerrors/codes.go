/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package hzerrors

type ErrorCode int32

const (
	ErrorCodeUndefined                        ErrorCode = 0
	ErrorCodeArrayIndexOutOfBounds            ErrorCode = 1
	ErrorCodeArrayStore                       ErrorCode = 2
	ErrorCodeAuthentication                   ErrorCode = 3
	ErrorCodeCache                            ErrorCode = 4
	ErrorCodeCacheLoader                      ErrorCode = 5
	ErrorCodeCacheNotExists                   ErrorCode = 6
	ErrorCodeCacheWriter                      ErrorCode = 7
	ErrorCodeCallerNotMember                  ErrorCode = 8
	ErrorCodeCancellation                     ErrorCode = 9
	ErrorCodeClassCast                        ErrorCode = 10
	ErrorCodeClassNotFound                    ErrorCode = 11
	ErrorCodeConcurrentModification           ErrorCode = 12
	ErrorCodeConfigMismatch                   ErrorCode = 13
	ErrorCodeConfiguration                    ErrorCode = 14
	ErrorCodeDistributedObjectDestroyed       ErrorCode = 15
	ErrorCodeDuplicateInstanceName            ErrorCode = 16
	ErrorCodeEOF                              ErrorCode = 17
	ErrorCodeEntryProcessor                   ErrorCode = 18
	ErrorCodeExecution                        ErrorCode = 19
	ErrorCodeHazelcast                        ErrorCode = 20
	ErrorCodeHazelcastInstanceNotActive       ErrorCode = 21
	ErrorCodeHazelcastOverLoad                ErrorCode = 22
	ErrorCodeHazelcastSerialization           ErrorCode = 23
	ErrorCodeIO                               ErrorCode = 24
	ErrorCodeIllegalArgument                  ErrorCode = 25
	ErrorCodeIllegalAccessException           ErrorCode = 26
	ErrorCodeIllegalAccessError               ErrorCode = 27
	ErrorCodeIllegalMonitorState              ErrorCode = 28
	ErrorCodeIllegalState                     ErrorCode = 29
	ErrorCodeIllegalThreadState               ErrorCode = 30
	ErrorCodeIndexOutOfBounds                 ErrorCode = 31
	ErrorCodeInterrupted                      ErrorCode = 32
	ErrorCodeInvalidAddress                   ErrorCode = 33
	ErrorCodeInvalidConfiguration             ErrorCode = 34
	ErrorCodeMemberLeft                       ErrorCode = 35
	ErrorCodeNegativeArraySize                ErrorCode = 36
	ErrorCodeNoSuchElement                    ErrorCode = 37
	ErrorCodeNotSerializable                  ErrorCode = 38
	ErrorCodeNilPointer                       ErrorCode = 39
	ErrorCodeOperationTimeout                 ErrorCode = 40
	ErrorCodePartitionMigrating               ErrorCode = 41
	ErrorCodeQuery                            ErrorCode = 42
	ErrorCodeQueryResultSizeExceeded          ErrorCode = 43
	ErrorCodeQuorum                           ErrorCode = 44
	ErrorCodeReachedMaxSize                   ErrorCode = 45
	ErrorCodeRejectedExecution                ErrorCode = 46
	ErrorCodeRemoteMapReduce                  ErrorCode = 47
	ErrorCodeResponseAlreadySent              ErrorCode = 48
	ErrorCodeRetryableHazelcast               ErrorCode = 49
	ErrorCodeRetryableIO                      ErrorCode = 50
	ErrorCodeRuntime                          ErrorCode = 51
	ErrorCodeSecurity                         ErrorCode = 52
	ErrorCodeSocket                           ErrorCode = 53
	ErrorCodeStaleSequence                    ErrorCode = 54
	ErrorCodeTargetDisconnected               ErrorCode = 55
	ErrorCodeTargetNotMember                  ErrorCode = 56
	ErrorCodeTimeout                          ErrorCode = 57
	ErrorCodeTopicOverload                    ErrorCode = 58
	ErrorCodeTopologyChanged                  ErrorCode = 59
	ErrorCodeTransaction                      ErrorCode = 60
	ErrorCodeTransactionNotActive             ErrorCode = 61
	ErrorCodeTransactionTimedOut              ErrorCode = 62
	ErrorCodeURISyntax                        ErrorCode = 63
	ErrorCodeUTFDataFormat                    ErrorCode = 64
	ErrorCodeUnsupportedOperation             ErrorCode = 65
	ErrorCodeWrongTarget                      ErrorCode = 66
	ErrorCodeXA                               ErrorCode = 67
	ErrorCodeAccessControl                    ErrorCode = 68
	ErrorCodeLogin                            ErrorCode = 69
	ErrorCodeUnsupportedCallback              ErrorCode = 70
	ErrorCodeNoDataMember                     ErrorCode = 71
	ErrorCodeReplicatedMapCantBeCreated       ErrorCode = 72
	ErrorCodeMaxMessageSizeExceeded           ErrorCode = 73
	ErrorCodeWANReplicationQueueFull          ErrorCode = 74
	ErrorCodeAssertionError                   ErrorCode = 75
	ErrorCodeOutOfMemoryError                 ErrorCode = 76
	ErrorCodeStackOverflowError               ErrorCode = 77
	ErrorCodeNativeOutOfMemoryError           ErrorCode = 78
	ErrorCodeNotFound                         ErrorCode = 79
	ErrorCodeStaleTaskID                      ErrorCode = 80
	ErrorCodeDuplicateTask                    ErrorCode = 81
	ErrorCodeStaleTask                        ErrorCode = 82
	ErrorCodeLocalMemberReset                 ErrorCode = 83
	ErrorCodeIndeterminateOperationState      ErrorCode = 84
	ErrorCodeFlakeIDNodeIDOutOfRangeException ErrorCode = 85
	ErrorCodeTargetNotReplicaException        ErrorCode = 86
	ErrorCodeMutationDisallowedException      ErrorCode = 87
	ErrorCodeConsistencyLostException         ErrorCode = 88
)
