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

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 * @author Public Transit Analytics
 */
public class InMemorySortedStore<K extends RangedKey<K>, V>
        implements RangedStore<K, V> {

    final NavigableMap<K, V> map;

    public InMemorySortedStore() {
        map = new ConcurrentSkipListMap<>();
    }

    @Override
    public SortedMap<K, V> getValuesInRange(final K min, final K max)
            throws InterruptedException, BitvantageStoreException {
        return map.subMap(min, true, max, true);
    }

    @Override
    public SortedMap<K, V> getValuesAbove(final K min) throws
            InterruptedException,
            BitvantageStoreException {
        return getValuesInRange(min, min.getRangeMax());
    }

    @Override
    public SortedMap<K, V> getValuesBelow(final K max)
            throws InterruptedException, BitvantageStoreException {
        return getValuesInRange(max.getRangeMin(), max);
    }

    @Override
    public boolean containsKey(final K key) throws BitvantageStoreException,
            InterruptedException {
        return map.containsKey(key);
    }

    @Override
    public V get(final K key) throws BitvantageStoreException,
            InterruptedException {
        return map.get(key);
    }

    @Override
    public void put(final K key, final V value) throws BitvantageStoreException,
            InterruptedException {
        map.put(key, value);
    }

    @Override
    public void delete(K key) throws BitvantageStoreException,
            InterruptedException {
        map.remove(key);
    }

    @Override
    public Multiset<V> getValues() throws BitvantageStoreException,
            InterruptedException {
        return ImmutableMultiset.copyOf(map.values());
    }

    @Override
    public boolean isEmpty() throws BitvantageStoreException,
            InterruptedException {
        return map.isEmpty();
    }

    @Override
    public int getMaxReaders() {
        return Integer.MAX_VALUE;
    }

    @Override
    public void close() {
    }

    @Override
    public void putRange(SortedMap<K, V> values) {
        map.putAll(values);
    }

}