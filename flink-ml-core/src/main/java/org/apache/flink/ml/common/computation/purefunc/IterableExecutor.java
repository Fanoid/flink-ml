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

package org.apache.flink.ml.common.computation.purefunc;

import org.apache.flink.util.Collector;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

class IterableExecutor {
    static <IN, OUT> Iterable<OUT> execute(Iterable<IN> in, MapPureFunc<IN, OUT> func) {
        return () -> new MapPureFuncIterator<>(in, func);
    }

    public static <IN, OUT> Iterable<OUT> execute(
            Iterable<IN> in, MapPartitionPureFunc<IN, OUT> func) {
        return () -> new MapPartitionPureFuncIterator<>(in, func);
    }

    static class MapPureFuncIterator<OUT, IN> implements Iterator<OUT> {
        private final Iterator<IN> iter;
        private final MapPureFunc<IN, OUT> fn;
        private final Collector<OUT> collector;
        private final Queue<OUT> output;

        public MapPureFuncIterator(Iterable<IN> in, MapPureFunc<IN, OUT> fn) {
            this.iter = in.iterator();
            this.fn = fn;
            output = new ArrayDeque<>();
            collector = new ConsumerCollector<>(output::add);
        }

        @Override
        public boolean hasNext() {
            while (output.isEmpty() && iter.hasNext()) {
                try {
                    fn.map(iter.next(), collector);
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Call map failed in %s", fn.getClass().getSimpleName()));
                }
            }
            return !output.isEmpty();
        }

        @Override
        public OUT next() {
            return output.poll();
        }
    }

    static class MapPartitionPureFuncIterator<OUT, IN> implements Iterator<OUT> {
        private final Iterable<IN> in;
        private final MapPartitionPureFunc<IN, OUT> fn;
        private final Collector<OUT> collector;
        private final Queue<OUT> output;

        public MapPartitionPureFuncIterator(Iterable<IN> in, MapPartitionPureFunc<IN, OUT> fn) {
            this.in = in;
            this.fn = fn;
            output = new ArrayDeque<>();
            collector = new ConsumerCollector<>(output::add);
        }

        @Override
        public boolean hasNext() {
            // TODO: Improve this with SynchronousQueue.
            fn.map(in, collector);
            return !output.isEmpty();
        }

        @Override
        public OUT next() {
            return output.poll();
        }
    }
}
