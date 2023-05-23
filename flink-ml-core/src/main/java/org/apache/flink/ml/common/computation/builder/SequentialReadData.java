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

package org.apache.flink.ml.common.computation.builder;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Represents a dataset with partition strategy.
 *
 * @param <T> The type of record.
 */
public class SequentialReadData<T> extends Data<Iterable<T>> {
    SequentialReadData(Data<T> data) {
        super(new IterableTypeInfo<>(data.type));
    }

    static final class IterableTypeInfo<T> extends TypeInformation<Iterable<T>> {

        private static final long serialVersionUID = 1L;

        private final TypeInformation<T> elementTypeInfo;

        public IterableTypeInfo(Class<T> elementTypeClass) {
            this.elementTypeInfo = of(checkNotNull(elementTypeClass, "elementTypeClass"));
        }

        public IterableTypeInfo(TypeInformation<T> elementTypeInfo) {
            this.elementTypeInfo = checkNotNull(elementTypeInfo, "elementTypeInfo");
        }

        // ------------------------------------------------------------------------
        //  IterableTypeInfo specific properties
        // ------------------------------------------------------------------------

        /** Gets the type information for the elements contained in the list. */
        public TypeInformation<T> getElementTypeInfo() {
            return elementTypeInfo;
        }

        // ------------------------------------------------------------------------
        //  TypeInformation implementation
        // ------------------------------------------------------------------------

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 0;
        }

        @Override
        public int getTotalFields() {
            // similar as arrays, the lists are "opaque" to the direct field addressing logic
            // since the list's elements are not addressable, we do not expose them
            return 1;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Class<Iterable<T>> getTypeClass() {
            return (Class<Iterable<T>>) (Class<?>) Iterable.class;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<Iterable<T>> createSerializer(ExecutionConfig config) {
            throw new UnsupportedOperationException();
        }

        // ------------------------------------------------------------------------

        @Override
        public String toString() {
            return "Iterable<" + elementTypeInfo + '>';
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            } else if (obj instanceof IterableTypeInfo) {
                final IterableTypeInfo<?> other = (IterableTypeInfo<?>) obj;
                return other.canEqual(this) && elementTypeInfo.equals(other.elementTypeInfo);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return 31 * elementTypeInfo.hashCode() + 1;
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj != null && obj.getClass() == getClass();
        }
    }
}
