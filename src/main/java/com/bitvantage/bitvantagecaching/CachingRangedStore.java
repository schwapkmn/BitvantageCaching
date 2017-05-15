/*
 * Copyright 2017 Public Transit Analytics.
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

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import java.util.Map;
import java.util.SortedMap;

/**
 *
 * @author Matt Laquidara
 */
public class CachingRangedStore<K extends RangedKey<K>, V>
        extends CachingStore<K, V> implements RangedStore<K, V> {

    private final RangedStore<K, V> store;
    private final RangedCache<K, V> cache;

    public CachingRangedStore(final RangedStore<K, V> store,
                              final RangedCache<K, V> cache) {
        super(store, cache);
        this.store = store;
        this.cache = cache;
    }

    @Override
    public SortedMap<K, V> getValuesInRange(final K min, final K max)
            throws InterruptedException, BitvantageStoreException {
        final RangeMap<K, RangeStatus<K, V>> response = cache.getRange(min, max);

        final ImmutableSortedMap.Builder<K, V> responseBuilder
                = new ImmutableSortedMap.Builder(Ordering.natural());

        for (final Map.Entry<Range<K>, RangeStatus<K, V>> entry : response
                .asMapOfRanges().entrySet()) {
            if (entry.getValue().isCached()) {
                responseBuilder.putAll(entry.getValue().getValues());
            } else {
                final SortedMap<K, V> values = store.getValuesInRange(
                        entry.getKey().lowerEndpoint(),
                        entry.getKey().upperEndpoint());

                responseBuilder.putAll(values);
                cache.putRange(min, max, values);
            }
        }

        return responseBuilder.build();
    }

    @Override
    public SortedMap<K, V> getValuesAbove(final K min) throws
            InterruptedException, BitvantageStoreException {
        return getValuesInRange(min, min.getRangeMax());
    }

    @Override
    public SortedMap<K, V> getValuesBelow(final K max) throws
            InterruptedException, BitvantageStoreException {
        return getValuesInRange(max.getRangeMin(), max);
    }

    @Override
    public void putRange(final SortedMap<K, V> values) 
            throws InterruptedException, BitvantageStoreException {
        store.putRange(values);
    }

}
