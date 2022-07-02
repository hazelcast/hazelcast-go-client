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

package nearcache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	ilogger "github.com/hazelcast/hazelcast-go-client/internal/logger"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

/*
ReparingTask runs on Near Cache side and only one instance is created per data-structure type like IMap and ICache.
Repairing responsibilities of this task are:

    * To scan RepairingHandlers to see if any Near Cache needs to be invalidated according to missed invalidation counts (controlled via MaxToleratedMissCount).
    * To send periodic generic-operations to cluster members in order to fetch latest partition sequences and UUIDs (controlled via InvalidationMinReconciliationIntervalSeconds.

See: com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask
*/
type ReparingTask struct {
	reconciliationIntervalNanos int64
	lastAntiEntropyRunNanos     int64
	handlers                    *sync.Map
	ss                          *serialization.Service
	ps                          *cluster.PartitionService
	doneCh                      <-chan struct{}
	invalidationMetaDataFetcher InvalidationMetaDataFetcher
	lg                          ilogger.LogAdaptor
	maxToleratedMissCount       int
	partitionCount              int32
	running                     int32
}

func NewReparingTask(recInt int, maxMissCnt int, ss *serialization.Service, ps *cluster.PartitionService, lg ilogger.LogAdaptor, mf InvalidationMetaDataFetcher, doneCh <-chan struct{}) *ReparingTask {
	nc := &ReparingTask{
		reconciliationIntervalNanos: int64(recInt) * 1_000_000_000,
		maxToleratedMissCount:       maxMissCnt,
		invalidationMetaDataFetcher: mf,
		doneCh:                      doneCh,
		ss:                          ss,
		ps:                          ps,
		lg:                          lg,
		handlers:                    &sync.Map{},
		partitionCount:              ps.PartitionCount(),
	}
	return nc
}

func (rt *ReparingTask) start() {
	// see: com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask#scheduleNextRun
	rt.lg.Debug(func() string {
		return "ReparingTask started"
	})
	timer := time.NewTicker(1 * time.Second)
	defer timer.Stop()
	rt.run()
	for {
		select {
		case <-rt.doneCh:
			return
		case <-timer.C:
			rt.run()
		}
	}
}

func (rt *ReparingTask) RegisterAndGetHandler(ctx context.Context, name string, nc *NearCache) (RepairingHandler, error) {
	handler := NewRepairingHandler(name, nc, rt.partitionCount, rt.ss, rt.ps, rt.lg)
	// ignoring the "loaded" return value
	h, loaded := rt.handlers.LoadOrStore(name, handler)
	if !loaded {
		if err := rt.invalidationMetaDataFetcher.Init(ctx, handler); err != nil {
			return RepairingHandler{}, nil
		}
		sr := NewStaleReadDetector(handler, rt.ps)
		nc.store.staleReadDetector = &sr
	}
	if atomic.CompareAndSwapInt32(&rt.running, 0, 1) {
		// this is the first added handler
		go rt.start()
		atomic.StoreInt64(&rt.lastAntiEntropyRunNanos, time.Now().UnixNano())
	}
	return h.(RepairingHandler), nil
}

func (rt *ReparingTask) DeregisterHandler(name string) {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask#deregisterHandler
	rt.handlers.Delete(name)
}

func (rt *ReparingTask) run() {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask#run
	rt.fixSequenceGaps()
	if rt.isAntiEntropyNeeded() {
		if err := rt.runAntiEntropy(context.Background()); err != nil {
			rt.lg.Errorf("%s", err.Error())
		}
		atomic.StoreInt64(&rt.lastAntiEntropyRunNanos, time.Now().UnixNano())
	}
}

// fixSequenceGaps marks relevant data as stale if missed invalidation event count is above the max tolerated miss count.
func (rt *ReparingTask) fixSequenceGaps() {
	rt.handlers.Range(func(_, value interface{}) bool {
		handler := value.(RepairingHandler)
		if rt.isAboveMaxToleratedMissCount(handler) {
			rt.updateLastKnownStaleSequences(handler)
		}
		return true
	})
}

func (rt *ReparingTask) isAboveMaxToleratedMissCount(handler RepairingHandler) bool {
	var total int64
	for i := int32(0); i < rt.partitionCount; i++ {
		md := handler.GetMetaDataContainer(i)
		total += md.MissedSequenceCount()
		if total > int64(rt.maxToleratedMissCount) {
			rt.lg.Trace(func() string {
				return fmt.Sprintf("above tolerated miss count:[map=%s,missCount=%d,maxToleratedMissCount=%d]",
					handler.Name(), total, rt.maxToleratedMissCount)
			})
			return true
		}
	}
	return false
}

func (rt *ReparingTask) updateLastKnownStaleSequences(handler RepairingHandler) {
	for i := int32(0); i < rt.partitionCount; i++ {
		md := handler.GetMetaDataContainer(i)
		mc := md.MissedSequenceCount()
		if mc != 0 {
			// return value is ignored.
			md.AddAndGetMissedSequenceCount(-mc)
			handler.UpdateLastKnownStaleSequence(md, i)
		}
	}
}

func (rt *ReparingTask) isAntiEntropyNeeded() bool {
	if rt.reconciliationIntervalNanos == 0 {
		return false
	}
	now := time.Now().UnixNano()
	since := now - atomic.LoadInt64(&rt.lastAntiEntropyRunNanos)
	rt.lg.Trace(func() string {
		return fmt.Sprintf("since last anti-entropy: %d, recon interval: %d, needed: %t",
			since, rt.reconciliationIntervalNanos, since >= rt.reconciliationIntervalNanos)
	})
	return since >= rt.reconciliationIntervalNanos
}

// runAntiEntropy periodically sends generic operations to cluster members to get latest invalidation metadata.
func (rt *ReparingTask) runAntiEntropy(ctx context.Context) error {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask#runAntiEntropy
	rt.lg.Debug(func() string {
		return "ReparingTask.runAntiEntropy"
	})
	// get a copy of the handlers, so we don't have to deal with sync.Map.
	handlers := map[string]RepairingHandler{}
	rt.handlers.Range(func(k, v interface{}) bool {
		handlers[k.(string)] = v.(RepairingHandler)
		return true
	})
	err := rt.invalidationMetaDataFetcher.fetchMetadata(ctx, handlers)
	if err != nil {
		return fmt.Errorf("ReparingTask: runAntiEntropy: %w", err)
	}
	return nil
}

// RepairingHandler is the port of: com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler
type RepairingHandler struct {
	name               string
	metadataContainers []*MetaDataContainer
	nc                 *NearCache
	toNearCacheKey     func(key interface{}) (interface{}, error)
	ss                 *serialization.Service
	ps                 *cluster.PartitionService
	lg                 ilogger.LogAdaptor
}

func NewRepairingHandler(name string, nc *NearCache, partitionCount int32, ss *serialization.Service, ps *cluster.PartitionService, lg ilogger.LogAdaptor) RepairingHandler {
	mcs := make([]*MetaDataContainer, partitionCount)
	for i := int32(0); i < partitionCount; i++ {
		mcs[i] = NewMetaDataContainer()
	}
	sk := nc.Config().SerializeKeys
	f := func(key interface{}) (interface{}, error) {
		if sk {
			return key, nil
		}
		return ss.ToObject(key.(serialization.Data))
	}
	return RepairingHandler{
		name:               name,
		nc:                 nc,
		metadataContainers: mcs,
		toNearCacheKey:     f,
		ss:                 ss,
		ps:                 ps,
		lg:                 lg,
	}
}

func (h RepairingHandler) Name() string {
	return h.name
}

func (h RepairingHandler) GetMetaDataContainer(partition int32) *MetaDataContainer {
	return h.metadataContainers[partition]
}

func (h *RepairingHandler) UpdateLastKnownStaleSequence(md *MetaDataContainer, partition int32) {
	var lastReceived int64
	var lastKnown int64
	for {
		lastReceived = md.Sequence()
		lastKnown = md.StaleSequence()
		if lastKnown >= lastReceived {
			break
		}
		if md.CASStaleSequence(lastKnown, lastReceived) {
			break
		}
	}
	h.lg.Trace(func() string {
		return fmt.Sprintf("stale sequences updated:[map=%s,partition=%d,lowerSequencesStaleThan=%d,lastReceivedSequence=%d]",
			h.name, partition, md.StaleSequence(), md.Sequence())
	})
}

// Handle handles a single invalidation.
func (h *RepairingHandler) Handle(key serialization.Data, source, partition types.UUID, seq int64) error {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler#handle(com.hazelcast.internal.serialization.Data, java.util.UUID, java.util.UUID, long)
	// Apply invalidation if it's not originated by local member/client.
	// Since local Near Caches are invalidated immediately there is no need to invalidate them twice.
	if source != partition {
		if key == nil {
			h.nc.Clear()
		} else {
			k, err := h.toNearCacheKey(key)
			if err != nil {
				return err
			}
			h.nc.Invalidate(k)
		}
	}
	pid, err := h.getPartitionIDOrDefault(key)
	if err != nil {
		return err
	}
	h.CheckOrRepairUUID(pid, partition)
	h.CheckOrRepairSequence(pid, seq, false)
	return nil
}

// HandleBatch handles a batch of validations.
func (h *RepairingHandler) HandleBatch(keys []serialization.Data, sources []types.UUID, partitions []types.UUID, seqs []int64) error {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler#handle(java.util.Collection<com.hazelcast.internal.serialization.Data>, java.util.Collection<java.util.UUID>, java.util.Collection<java.util.UUID>, java.util.Collection<java.lang.Long>)
	// assumes len(keys) == len(source) -- len(partitions) == len(seqs)
	sz := len(keys)
	for i := 0; i < sz; i++ {
		if err := h.Handle(keys[i], sources[i], partitions[i], seqs[i]); err != nil {
			return err
		}
	}
	return nil
}

func (h *RepairingHandler) CheckOrRepairUUID(partition int32, new types.UUID) {
	// this method may be called concurrently: anti-entropy, event service
	if new.Default() {
		panic("CheckOrRepairUUID: new UUID should not be the default UUID")
	}
	md := h.GetMetaDataContainer(partition)
	for {
		prev := md.UUID()
		if prev == new {
			break
		}
		if md.CASUUID(prev, new) {
			md.ResetStaleSequence()
			md.ResetStaleSequence()
			h.lg.Trace(func() string {
				return fmt.Sprintf("invalid UUID, lost remote partition data unexpectedly:[name=%s,partition=%d,prevUuid=%s,newUuid=%s]",
					h.name, partition, prev, new)
			})
			break
		}
	}
}

func (h *RepairingHandler) CheckOrRepairSequence(partition int32, nextSeq int64, viaAntiEntropy bool) {
	if nextSeq <= 0 {
		panic("CheckOrRepairSequence <= 0")
	}
	md := h.GetMetaDataContainer(partition)
	for {
		curSeq := md.Sequence()
		if curSeq >= nextSeq {
			break
		}
		if md.CASSequence(curSeq, nextSeq) {
			diff := nextSeq - curSeq
			if viaAntiEntropy || diff > 1 {
				// We have found at least one missing sequence between current and next sequences.
				// If miss is detected by anti-entropy, number of missed sequences will be miss = next - current.
				// Otherwise it means miss is detected by observing received invalidation event sequence numbers and number of missed sequences will be miss = next - current - 1.
				missCnt := diff
				if !viaAntiEntropy {
					missCnt -= 1
				}
				total := md.AddAndGetMissedSequenceCount(missCnt)
				h.lg.Trace(func() string {
					return fmt.Sprintf("invalid sequence:[map=%s,partition=%d,currentSequence=%d,nextSequence=%d,totalMissCount=%d]",
						h.name, partition, curSeq, nextSeq, total)
				})
			}
			break
		}
	}
}

func (h RepairingHandler) getPartitionIDOrDefault(key serialization.Data) (int32, error) {
	if key == nil {
		// name is used to determine partition ID of map-wide events like clear()
		// since key is nil, we are using name to find the partition ID
		data, err := h.ss.ToData(h.name)
		if err != nil {
			return 0, err
		}
		key = data
	}
	return h.ps.GetPartitionID(key)
}

// MetaDataContainer contains one partitions' invalidation metadata.
// port of: com.hazelcast.internal.nearcache.impl.invalidation.MetaDataContainer
type MetaDataContainer struct {
	seq        int64
	staleSeq   int64
	missedSeqs int64
	uuid       atomic.Value
}

func NewMetaDataContainer() *MetaDataContainer {
	uuid := atomic.Value{}
	uuid.Store(types.UUID{})
	return &MetaDataContainer{
		uuid: uuid,
	}
}

func (mc *MetaDataContainer) SetUUID(uuid types.UUID) {
	mc.uuid.Store(uuid)
}

func (mc MetaDataContainer) UUID() types.UUID {
	return mc.uuid.Load().(types.UUID)
}

func (mc *MetaDataContainer) CASUUID(prev, new types.UUID) bool {
	return mc.uuid.CompareAndSwap(prev, new)
}

func (mc *MetaDataContainer) SetSequence(seq int64) {
	atomic.StoreInt64(&mc.seq, seq)
}

func (mc MetaDataContainer) Sequence() int64 {
	return atomic.LoadInt64(&mc.seq)
}

func (mc *MetaDataContainer) ResetSequence() {
	mc.SetSequence(0)
}

func (mc *MetaDataContainer) CASSequence(current, next int64) bool {
	return atomic.CompareAndSwapInt64(&mc.seq, current, next)
}

func (mc *MetaDataContainer) SetStaleSequence(seq int64) {
	atomic.StoreInt64(&mc.staleSeq, seq)
}

func (mc MetaDataContainer) StaleSequence() int64 {
	return atomic.LoadInt64(&mc.staleSeq)
}

func (mc *MetaDataContainer) ResetStaleSequence() {
	mc.SetStaleSequence(0)
}

func (mc *MetaDataContainer) CASStaleSequence(lastKnown, lastReceived int64) bool {
	return atomic.CompareAndSwapInt64(&mc.staleSeq, lastKnown, lastReceived)
}

func (mc *MetaDataContainer) SetMissedSequenceCount(count int64) {
	atomic.StoreInt64(&mc.missedSeqs, count)
}

func (mc MetaDataContainer) MissedSequenceCount() int64 {
	return atomic.LoadInt64(&mc.missedSeqs)
}

func (mc *MetaDataContainer) AddAndGetMissedSequenceCount(count int64) int64 {
	return atomic.AddInt64(&mc.missedSeqs, count)
}

// InvalidationMetaDataFetcher runs on Near Cache side.
// An instance of this task is responsible for fetching of all Near Caches' remote metadata like last sequence numbers and partition UUIDs.
// port of: com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher
// port of: com.hazelcast.client.map.impl.nearcache.invalidation.ClientMapInvalidationMetaDataFetcher
type InvalidationMetaDataFetcher struct {
	cs         *cluster.Service
	is         *invocation.Service
	invFactory *cluster.ConnectionInvocationFactory
	lg         ilogger.LogAdaptor
}

func NewInvalidationMetaDataFetcher(cs *cluster.Service, is *invocation.Service, invFactory *cluster.ConnectionInvocationFactory, lg ilogger.LogAdaptor) InvalidationMetaDataFetcher {
	df := InvalidationMetaDataFetcher{
		cs:         cs,
		is:         is,
		invFactory: invFactory,
		lg:         lg,
	}
	return df
}

func (df InvalidationMetaDataFetcher) Init(ctx context.Context, handler RepairingHandler) error {
	handlers := map[string]RepairingHandler{
		handler.Name(): handler,
	}
	return df.fetchMetadata(ctx, handlers)
}

func (df InvalidationMetaDataFetcher) fetchMetadata(ctx context.Context, handlers map[string]RepairingHandler) error {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher#fetchMetadata
	if len(handlers) == 0 {
		return nil
	}
	// getDataStructureNames
	names := []string{}
	for _, h := range handlers {
		names = append(names, h.Name())
	}
	invs, err := df.fetchMembersMetadataFor(ctx, names)
	if err != nil {
		return fmt.Errorf("InvalidationMetaDataFetcher.fetchMetadata: fetching members metadata: %w", err)
	}
	for _, inv := range invs {
		if err := df.processMemberMetadata(ctx, inv, handlers); err != nil {
			return fmt.Errorf("InvalidationMetaDataFetcher.fetchMetadata: processing metadata: %w", err)
		}
	}
	return nil
}

func (df InvalidationMetaDataFetcher) fetchMembersMetadataFor(ctx context.Context, names []string) (map[types.UUID]invocation.Invocation, error) {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher#fetchMembersMetadataFor
	mems := filterDataMembers(df.cs.OrderedMembers())
	if len(mems) == 0 {
		return nil, nil
	}
	invs := make(map[types.UUID]invocation.Invocation, len(mems))
	for _, mem := range mems {
		inv, err := df.fetchMetaDataOf(ctx, mem, names)
		if err != nil {
			return nil, err
		}
		invs[mem.UUID] = inv
	}
	return invs, nil
}

func (df InvalidationMetaDataFetcher) fetchMetaDataOf(ctx context.Context, mem pubcluster.MemberInfo, names []string) (*cluster.MemberBoundInvocation, error) {
	// port of: com.hazelcast.client.map.impl.nearcache.invalidation.ClientMapInvalidationMetaDataFetcher#fetchMetadataOf
	msg := codec.EncodeMapFetchNearCacheInvalidationMetadataRequest(names, mem.UUID)
	inv := df.invFactory.NewMemberBoundInvocation(msg, &mem, time.Now())
	if err := df.is.SendRequest(ctx, inv); err != nil {
		return nil, err
	}
	return inv, nil
}

func (df *InvalidationMetaDataFetcher) processMemberMetadata(ctx context.Context, inv invocation.Invocation, handlers map[string]RepairingHandler) error {
	// port of: com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher#processMemberMetadata
	// extractMemberMetadata
	res, err := inv.GetWithContext(ctx)
	if err != nil {
		return err
	}
	npsPairs, psPairs := codec.DecodeMapFetchNearCacheInvalidationMetadataResponse(res)
	// repairUuids
	// see: com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher#repairUuids
	for _, p := range psPairs {
		k := p.Key.(int32)
		v := p.Value.(types.UUID)
		for _, handler := range handlers {
			handler.CheckOrRepairUUID(k, v)
		}
	}
	// repairSequences
	// see: com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher#repairSequences
	for _, np := range npsPairs {
		handler := handlers[np.Key.(string)]
		vvs := np.Value.([]proto.Pair)
		for _, vv := range vvs {
			k := vv.Key.(int32)
			v := vv.Value.(int64)
			handler.CheckOrRepairSequence(k, v, true)
		}
	}
	return nil
}

// filterDataMembers removes lite members from the given slice.
// Order of the members is not preserved.
func filterDataMembers(mems []pubcluster.MemberInfo) []pubcluster.MemberInfo {
	di := len(mems)
loop:
	for i := 0; i < di; i++ {
		if mems[i].LiteMember {
			// order is not important, delete by moving deleted items to the end.
			// find the first non-lite member
			di--
			for mems[di].LiteMember {
				if di <= i {
					// no more data members left
					break loop
				}
				di--
			}
			mems[i], mems[di] = mems[di], mems[i]
		}
	}
	// all deleted items are at the end.
	// shrink the slice to get rid of them.
	return mems[:di]
}
