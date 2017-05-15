/*
 * Copyright 2017 Matt Laquidara.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bitvantage.bitvantagecaching;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.util.SortedMap;

/**
 *
 * @author Matt Laquidara
 */
public class UnboundedRangedCache<K extends RangedKey<K>, V>
        implements RangedCache<K, V> {

    private final TreeRangeSet<K> requestedRanges;
    private final RangedStore<K, V> store;

    public UnboundedRangedCache(final RangedStore store) {
        this.store = store;
        requestedRanges = TreeRangeSet.create();
    }

    @Override
    public RangeMap<K, RangeStatus<K, V>> getRange(final K min, final K max)
            throws InterruptedException, BitvantageStoreException {
        final Range<K> requestRange = Range.open(min, max);
        if (requestedRanges.encloses(requestRange)) {
            final SortedMap<K, V> values = ImmutableSortedMap.copyOf(
                    store.getValuesInRange(min, max));
            return ImmutableRangeMap.of(requestRange,
                                        new RangeStatus(true, values));
        } else {
            final RangeSet<K> cachedSubRanges;
            synchronized (this) {
                cachedSubRanges = ImmutableRangeSet.copyOf(
                        requestedRanges.subRangeSet(requestRange));
            }
            final ImmutableRangeMap.Builder<K, RangeStatus<K, V>> rangeMapBuilder
                    = ImmutableRangeMap.builder();
            for (final Range<K> subRange : cachedSubRanges.asRanges()) {
                final SortedMap<K, V> values = ImmutableSortedMap.copyOf(
                        store.getValuesInRange(subRange.lowerEndpoint(),
                                               subRange.upperEndpoint()));
                rangeMapBuilder.put(subRange, new RangeStatus(true, values));
            }
            final RangeSet<K> uncachedSubRanges;
            synchronized (this) {
                uncachedSubRanges = ImmutableRangeSet.copyOf(
                        requestedRanges.complement().subRangeSet(requestRange));
            }
            for (final Range<K> subRange : uncachedSubRanges.asRanges()) {
                rangeMapBuilder.put(subRange, new RangeStatus(false, null));
            }
            return rangeMapBuilder.build();
        }
    }

    @Override
    public void putRange(
            final K requestedMin, K requestedMax, final SortedMap<K, V> values)
            throws InterruptedException, BitvantageStoreException {
        store.putRange(values);
        synchronized (this) {
            requestedRanges.add(Range.closed(requestedMin, requestedMax));
        }
    }

    @Override
    public V get(K key) throws InterruptedException, BitvantageStoreException {
        return store.get(key);
    }

    @Override
    public void put(K key, V value) throws InterruptedException,
            BitvantageStoreException {
        store.put(key, value);
        synchronized (this) {
            requestedRanges.add(Range.singleton(key));
        }
    }

    @Override
    public void invalidate(K key) throws InterruptedException,
            BitvantageStoreException {
        synchronized (this) {
            requestedRanges.remove(Range.singleton(key));
        }
        store.delete(key);
    }

    @Override
    public void close() {
        store.close();
    }

}
