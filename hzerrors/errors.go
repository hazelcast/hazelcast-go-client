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

import "errors"

var (
	ErrClientOffline                    = errors.New("client offline error")
	ErrClientNotAllowedInCluster        = errors.New("client not allowed in cluster error")
	ErrClientNotActive                  = errors.New("client not active error")
	ErrAddressNotFound                  = errors.New("address not found error")
	ErrUndefined                        = errors.New("undefined error")
	ErrArrayIndexOutOfBounds            = errors.New("array index out of bounds error")
	ErrArrayStore                       = errors.New("array store error")
	ErrAuthentication                   = errors.New("authentication error")
	ErrCache                            = errors.New("cache error")
	ErrCacheLoader                      = errors.New("cache loader error")
	ErrCacheNotExists                   = errors.New("cache not exists error")
	ErrCacheWriter                      = errors.New("cache writer error")
	ErrCallerNotMember                  = retryable("caller not member error")
	ErrCancellation                     = errors.New("cancellation error")
	ErrClassCast                        = errors.New("class cast error")
	ErrClassNotFound                    = errors.New("class not found error")
	ErrConcurrentModification           = errors.New("concurrent modification error")
	ErrConfigMismatch                   = errors.New("config mismatch error")
	ErrDistributedObjectDestroyed       = errors.New("distributed object destroyed error")
	ErrEOF                              = errors.New("eof error")
	ErrEntryProcessor                   = errors.New("entry processor error")
	ErrExecution                        = errors.New("execution error")
	ErrHazelcast                        = errors.New("hazelcast error")
	ErrHazelcastInstanceNotActive       = errors.New("hazelcast instance not active error")
	ErrHazelcastOverLoad                = errors.New("hazelcast overload error")
	ErrHazelcastSerialization           = errors.New("hazelcast serialization error")
	ErrIO                               = errors.New("io error")
	ErrIllegalArgument                  = errors.New("illegal argument error")
	ErrIllegalAccessException           = errors.New("illegal access exception")
	ErrIllegalAccess                    = errors.New("illegal access error")
	ErrIllegalMonitorState              = errors.New("illegal monitor state error")
	ErrIllegalState                     = errors.New("illegal state error")
	ErrIllegalThreadState               = errors.New("illegal thread state error")
	ErrIndexOutOfBounds                 = errors.New("index out of bounds error")
	ErrInterrupted                      = errors.New("interrupted error")
	ErrInvalidAddress                   = errors.New("invalid address error")
	ErrInvalidConfiguration             = errors.New("invalid configuration error")
	ErrMemberLeft                       = retryable("member left error")
	ErrNegativeArraySize                = errors.New("negative array size")
	ErrNoSuchElement                    = errors.New("no such element errror")
	ErrNotSerializable                  = errors.New("not serializable error")
	ErrNilPointer                       = errors.New("nil pointer error")
	ErrOperationTimeout                 = errors.New("operation timeout error")
	ErrPartitionMigrating               = retryable("partition migrating error")
	ErrQuery                            = errors.New("query error")
	ErrQueryResultSizeExceeded          = errors.New("query result size exceeded error")
	ErrSplitBrainProtection             = errors.New("split brain protection error")
	ErrReachedMaxSize                   = errors.New("reached max size error")
	ErrRejectedExecution                = errors.New("rejected execution error")
	ErrResponseAlreadySent              = errors.New("response already sent error")
	ErrRetryableHazelcast               = retryable("retryable hazelcast error")
	ErrRetryableIO                      = retryable("retryable io error")
	ErrRuntime                          = errors.New("runtime error")
	ErrSecurity                         = errors.New("security error")
	ErrSocket                           = errors.New("socket error")
	ErrStaleSequence                    = errors.New("stalesequence error")
	ErrTargetDisconnected               = errors.New("target disconnected error")
	ErrTargetNotMember                  = retryable("target not member error")
	ErrTimeout                          = errors.New("timeout error")
	ErrTopicOverload                    = errors.New("topic overload error")
	ErrTransaction                      = errors.New("transaction error")
	ErrTransactionNotActive             = errors.New("transaction not active error")
	ErrTransactionTimedOut              = errors.New("transaction timed out error")
	ErrURISyntax                        = errors.New("uri syntax error")
	ErrUTFDataFormat                    = errors.New("utf data format error")
	ErrUnsupportedOperation             = errors.New("unsupported operation error")
	ErrWrongTarget                      = retryable("wrong target error")
	ErrXA                               = errors.New("xa error")
	ErrAccessControl                    = errors.New("access control error")
	ErrLogin                            = errors.New("login error")
	ErrUnsupportedCallback              = errors.New("unsupported callback error")
	ErrNoDataMember                     = errors.New("no data member error")
	ErrReplicatedMapCantBeCreated       = errors.New("replicated map cant be created error")
	ErrMaxMessageSizeExceeded           = errors.New("max message sized exceeded error")
	ErrWANReplicationQueueFull          = errors.New("wan replication query full error")
	ErrAssertion                        = errors.New("assertion error")
	ErrOutOfMemory                      = errors.New("out of memory error")
	ErrStackOverflow                    = errors.New("stack overflow error")
	ErrNativeOutOfMemory                = errors.New("native out of memory error")
	ErrServiceNotFound                  = errors.New("service not found error")
	ErrStaleTaskID                      = errors.New("stale task id error")
	ErrDuplicateTask                    = errors.New("duplicate task error")
	ErrStaleTask                        = errors.New("stale task error")
	ErrLocalMemberReset                 = errors.New("member reset error")
	ErrIndeterminateOperationState      = errors.New("indeterminate operation state error")
	ErrFlakeIDNodeIDOutOfRangeException = errors.New("flake id nodeid out of range exception")
	ErrTargetNotReplicaException        = retryable("target not replica exception")
	ErrMutationDisallowedException      = errors.New("mutation disallowed exception")
	ErrConsistencyLostException         = errors.New("consistency lost error")
	ErrSessionExpiredException          = errors.New("session expired exception")
	ErrWaitKeyCancelledException        = errors.New("wait key cancelled exception")
	ErrLockAcquireLimitReachedException = errors.New("lock acquire limit reached exception")
	ErrLockOwnershipLostException       = errors.New("lock ownership lost exception")
	ErrCPGroupDestroyedException        = errors.New("cp group destroyed exception")
	ErrCannotReplicateException         = retryable("cannot replicate exception")
	ErrLeaderDemotedException           = errors.New("leader demoted exception")
	ErrStaleAppendRequestException      = errors.New("stale append request exception")
	ErrNotLeaderException               = errors.New("not leader exception")
	ErrVersionMismatchException         = errors.New("version mismatch exception")
	ErrNoSuchMethod                     = errors.New("no such method exception")
	ErrNoSuchMethodException            = errors.New("no such method exception")
	ErrNoSuchField                      = errors.New("no such field error")
	ErrNoSuchFieldException             = errors.New("no such field exception")
	ErrNoClassDefFound                  = errors.New("no class def found error")
)

type RetryableError string

func retryable(s string) error {
	e := RetryableError(s)
	return &e
}

func (e RetryableError) Error() string {
	return string(e)
}
