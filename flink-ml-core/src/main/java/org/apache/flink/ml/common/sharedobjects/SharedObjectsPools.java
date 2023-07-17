/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.common.sharedobjects;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.iteration.datacache.nonkeyed.ListStateWithCache;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Stores and manages all shared objects. Every shared object is identified by a tuple of (Pool ID,
 * subtask ID, name). Every call of {@link SharedObjectsUtils#withSharedObjects} generated a
 * different {@link PoolID}, so that they do not interfere with each other.
 */
class SharedObjectsPools {

    private static final Logger LOG = LoggerFactory.getLogger(SharedObjectsPools.class);

    // Stores values of all shared objects, including their corresponding epochs.
    private static final Map<Tuple3<PoolID, Integer, String>, Tuple2<Integer, Object>> values =
            new ConcurrentHashMap<>();

    private static final Map<Tuple3<PoolID, Integer, String>, List<Tuple2<Integer, CountDownLatch>>>
            waitQueues = new ConcurrentHashMap<>();

    /**
     * Stores owners of all shared objects, where the owner is identified by the accessor ID
     * obtained from {@link SharedObjectsStreamOperator#getSharedObjectsAccessorID()}.
     */
    private static final Map<Tuple3<PoolID, Integer, String>, String> owners =
            new ConcurrentHashMap<>();

    // Stores number of references of all shared objects. A shared object is removed when its number
    // of references decreased to 0.
    private static final ConcurrentHashMap<Tuple3<PoolID, Integer, String>, Integer> refCounters =
            new ConcurrentHashMap<>();

    @SuppressWarnings("UnusedReturnValue")
    static int incRefCount(Tuple3<PoolID, Integer, String> t) {
        return refCounters.compute(t, (k, oldV) -> null == oldV ? 1 : oldV + 1);
    }

    @SuppressWarnings("UnusedReturnValue")
    static int decRefCount(Tuple3<PoolID, Integer, String> t) {
        //noinspection DataFlowIssue
        int refCount = refCounters.compute(t, (k, oldV) -> oldV - 1);
        if (refCount == 0) {
            values.remove(t);
            waitQueues.remove(t);
            owners.remove(t);
            refCounters.remove(t);
        }
        return refCount;
    }

    /** Gets a {@link Reader} of shared item identified by (PoolID, subtaskId, descriptor). */
    static <T> Reader<T> getReader(PoolID poolID, int subtaskId, ItemDescriptor<T> descriptor) {
        return new Reader<>(Tuple3.of(poolID, subtaskId, descriptor.name));
    }

    /** Gets a {@link Writer} of a shared object. */
    static <T> Writer<T> getWriter(
            PoolID poolId,
            int subtaskId,
            ItemDescriptor<T> descriptor,
            String ownerId,
            OperatorID operatorID,
            StreamTask<?, ?> containingTask,
            StreamingRuntimeContext runtimeContext,
            StateInitializationContext stateInitializationContext,
            int epoch) {
        Tuple3<PoolID, Integer, String> objId = Tuple3.of(poolId, subtaskId, descriptor.name);
        String lastOwner = owners.putIfAbsent(objId, ownerId);
        if (null != lastOwner) {
            throw new IllegalStateException(
                    String.format(
                            "The shared item (%s, %s, %s) already has a writer %s.",
                            poolId, subtaskId, descriptor.name, ownerId));
        }
        Writer<T> writer =
                new Writer<>(
                        objId,
                        ownerId,
                        descriptor.serializer,
                        containingTask,
                        runtimeContext,
                        stateInitializationContext,
                        operatorID);
        if (null != descriptor.initVal) {
            writer.set(descriptor.initVal, epoch);
        }
        return writer;
    }

    static class Reader<T> {
        protected final Tuple3<PoolID, Integer, String> objId;

        Reader(Tuple3<PoolID, Integer, String> objId) {
            this.objId = objId;
            incRefCount(objId);
        }

        private T getInternal(int fetchEpoch, boolean hasTimeout, long timeout, TimeUnit unit)
                throws InterruptedException {
            //noinspection unchecked
            Tuple2<Integer, T> epochV = (Tuple2<Integer, T>) values.get(objId);
            if (null != epochV) {
                int currentEpoch = epochV.f0;
                LOG.debug(
                        "Get {} with epoch {}, current epoch is {}",
                        objId,
                        fetchEpoch,
                        currentEpoch);
                Preconditions.checkState(
                        currentEpoch <= fetchEpoch,
                        String.format(
                                "Current epoch %d of %s is larger than fetch epoch %d.",
                                currentEpoch, objId, fetchEpoch));
                if (fetchEpoch == epochV.f0) {
                    return epochV.f1;
                }
            }
            CountDownLatch latch = new CountDownLatch(1);
            synchronized (waitQueues) {
                if (!waitQueues.containsKey(objId)) {
                    waitQueues.put(objId, new ArrayList<>());
                }
                List<Tuple2<Integer, CountDownLatch>> q = waitQueues.get(objId);
                q.add(Tuple2.of(fetchEpoch, latch));
            }
            if (hasTimeout) {
                if (latch.await(timeout, unit)) {
                    //noinspection unchecked
                    epochV = (Tuple2<Integer, T>) values.get(objId);
                    Preconditions.checkState(epochV.f0 == fetchEpoch);
                    return epochV.f1;
                } else {
                    return null;
                }
            } else {
                latch.await();
                //noinspection unchecked
                epochV = (Tuple2<Integer, T>) values.get(objId);
                Preconditions.checkState(epochV.f0 == fetchEpoch);
                return epochV.f1;
            }
        }

        T get(int fetchEpoch) throws InterruptedException {
            return getInternal(fetchEpoch, false, 0, TimeUnit.MILLISECONDS);
        }

        T get(int fetchEpoch, long timeout, TimeUnit unit) throws InterruptedException {
            return getInternal(fetchEpoch, true, timeout, unit);
        }

        void remove() {
            decRefCount(objId);
        }
    }

    static class Writer<T> extends Reader<T> {
        private final String ownerId;
        private final ListStateWithCache<Tuple2<Integer, T>> cache;
        private boolean isDirty;

        Writer(
                Tuple3<PoolID, Integer, String> itemId,
                String ownerId,
                TypeSerializer<T> serializer,
                StreamTask<?, ?> containingTask,
                StreamingRuntimeContext runtimeContext,
                StateInitializationContext stateInitializationContext,
                OperatorID operatorID) {
            super(itemId);
            this.ownerId = ownerId;
            try {
                //noinspection unchecked
                cache =
                        new ListStateWithCache<>(
                                new TupleSerializer<>(
                                        (Class<Tuple2<Integer, T>>) (Class<?>) Tuple2.class,
                                        new TypeSerializer[] {IntSerializer.INSTANCE, serializer}),
                                containingTask,
                                runtimeContext,
                                stateInitializationContext,
                                operatorID);
                Iterator<Tuple2<Integer, T>> iterator = cache.get().iterator();
                if (iterator.hasNext()) {
                    Tuple2<Integer, T> epochV = iterator.next();
                    ensureOwner();
                    //noinspection unchecked
                    values.put(itemId, (Tuple2<Integer, Object>) epochV);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            isDirty = false;
        }

        private void ensureOwner() {
            // Double-checks the owner, because a writer may call this method after the key removed
            // and re-added by other operators.
            Preconditions.checkState(owners.get(objId).equals(ownerId));
        }

        void set(T value, int epoch) {
            ensureOwner();
            values.put(objId, Tuple2.of(epoch, value));
            LOG.debug("Set {} with epoch {}", objId, epoch);
            isDirty = true;
            synchronized (waitQueues) {
                if (!waitQueues.containsKey(objId)) {
                    waitQueues.put(objId, new ArrayList<>());
                }
                List<Tuple2<Integer, CountDownLatch>> q = waitQueues.get(objId);
                ListIterator<Tuple2<Integer, CountDownLatch>> iter = q.listIterator();
                while (iter.hasNext()) {
                    Tuple2<Integer, CountDownLatch> next = iter.next();
                    if (epoch == next.f0) {
                        iter.remove();
                        next.f1.countDown();
                    }
                }
            }
        }

        @Override
        void remove() {
            ensureOwner();
            super.remove();
            cache.clear();
        }

        void snapshotState(StateSnapshotContext context) throws Exception {
            if (isDirty) {
                //noinspection unchecked
                cache.update(Collections.singletonList((Tuple2<Integer, T>) values.get(objId)));
                isDirty = false;
            }
            cache.snapshotState(context);
        }
    }
}
