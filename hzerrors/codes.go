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

type errorCode int32

const (
	errorCodeUndefined                        errorCode = 0
	errorCodeArrayIndexOutOfBounds            errorCode = 1
	errorCodeArrayStore                       errorCode = 2
	errorCodeAuthentication                   errorCode = 3
	errorCodeCache                            errorCode = 4
	errorCodeCacheLoader                      errorCode = 5
	errorCodeCacheNotExists                   errorCode = 6
	errorCodeCacheWriter                      errorCode = 7
	errorCodeCallerNotMember                  errorCode = 8
	errorCodeCancellation                     errorCode = 9
	errorCodeClassCast                        errorCode = 10
	errorCodeClassNotFound                    errorCode = 11
	errorCodeConcurrentModification           errorCode = 12
	errorCodeConfigMismatch                   errorCode = 13
	errorCodeDistributedObjectDestroyed       errorCode = 14
	errorCodeEOF                              errorCode = 15
	errorCodeEntryProcessor                   errorCode = 16
	errorCodeExecution                        errorCode = 17
	errorCodeHazelcast                        errorCode = 18
	errorCodeHazelcastInstanceNotActive       errorCode = 19
	errorCodeHazelcastOverLoad                errorCode = 20
	errorCodeHazelcastSerialization           errorCode = 21
	errorCodeIO                               errorCode = 22
	errorCodeIllegalArgument                  errorCode = 23
	errorCodeIllegalAccessException           errorCode = 24
	errorCodeIllegalAccess                    errorCode = 25
	errorCodeIllegalMonitorState              errorCode = 26
	errorCodeIllegalState                     errorCode = 27
	errorCodeIllegalThreadState               errorCode = 28
	errorCodeIndexOutOfBounds                 errorCode = 29
	errorCodeInterrupted                      errorCode = 30
	errorCodeInvalidAddress                   errorCode = 31
	errorCodeInvalidConfiguration             errorCode = 32
	errorCodeMemberLeft                       errorCode = 33
	errorCodeNegativeArraySize                errorCode = 34
	errorCodeNoSuchElement                    errorCode = 35
	errorCodeNotSerializable                  errorCode = 36
	errorCodeNilPointer                       errorCode = 37
	errorCodeOperationTimeout                 errorCode = 38
	errorCodePartitionMigrating               errorCode = 39
	errorCodeQuery                            errorCode = 40
	errorCodeQueryResultSizeExceeded          errorCode = 41
	errorCodeSplitBrainProtection             errorCode = 42
	errorCodeReachedMaxSize                   errorCode = 43
	errorCodeRejectedExecution                errorCode = 44
	errorCodeResponseAlreadySent              errorCode = 45
	errorCodeRetryableHazelcast               errorCode = 46
	errorCodeRetryableIO                      errorCode = 47
	errorCodeRuntime                          errorCode = 48
	errorCodeSecurity                         errorCode = 49
	errorCodeSocket                           errorCode = 50
	errorCodeStaleSequence                    errorCode = 51
	errorCodeTargetDisconnected               errorCode = 52
	errorCodeTargetNotMember                  errorCode = 53
	errorCodeTimeout                          errorCode = 54
	errorCodeTopicOverload                    errorCode = 55
	errorCodeTransaction                      errorCode = 56
	errorCodeTransactionNotActive             errorCode = 57
	errorCodeTransactionTimedOut              errorCode = 58
	errorCodeURISyntax                        errorCode = 59
	errorCodeUTFDataFormat                    errorCode = 60
	errorCodeUnsupportedOperation             errorCode = 61
	errorCodeWrongTarget                      errorCode = 62
	errorCodeXA                               errorCode = 63
	errorCodeAccessControl                    errorCode = 64
	errorCodeLogin                            errorCode = 65
	errorCodeUnsupportedCallback              errorCode = 66
	errorCodeNoDataMember                     errorCode = 67
	errorCodeReplicatedMapCantBeCreated       errorCode = 68
	errorCodeMaxMessageSizeExceeded           errorCode = 69
	errorCodeWANReplicationQueueFull          errorCode = 70
	errorCodeAssertionError                   errorCode = 71
	errorCodeOutOfMemoryError                 errorCode = 72
	errorCodeStackOverflowError               errorCode = 73
	errorCodeNativeOutOfMemoryError           errorCode = 74
	errorCodeServiceNotFound                  errorCode = 75
	errorCodeStaleTaskID                      errorCode = 76
	errorCodeDuplicateTask                    errorCode = 77
	errorCodeStaleTask                        errorCode = 78
	errorCodeLocalMemberReset                 errorCode = 79
	errorCodeIndeterminateOperationState      errorCode = 80
	errorCodeFlakeIDNodeIDOutOfRangeException errorCode = 81
	errorCodeTargetNotReplicaException        errorCode = 82
	errorCodeMutationDisallowedException      errorCode = 83
	errorCodeConsistencyLostException         errorCode = 84
	errorSessionExpiredException              errorCode = 85
	errorWaitKeyCancelledException            errorCode = 86
	errorLockAcquireLimitReachedException     errorCode = 87
	errorLockOwnershipLostException           errorCode = 88
	errorCPGroupDestroyedException            errorCode = 89
	errorCannotReplicateException             errorCode = 90
	errorLeaderDemotedException               errorCode = 91
	errorStaleAppendRequestException          errorCode = 92
	errorNotLeaderException                   errorCode = 93
	errorVersionMismatchException             errorCode = 94
	errorNoSuchMethod                         errorCode = 95
	errorNoSuchMethodException                errorCode = 96
	errorNoSuchField                          errorCode = 97
	errorNoSuchFieldException                 errorCode = 98
	errorNoClassDefFound                      errorCode = 99
)
