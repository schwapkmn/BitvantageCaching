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
package com.bitvantage.bitvantagecaching.mocks;

import com.bitvantage.bitvantagecaching.RangedKey;
import com.bitvantage.bitvantagecaching.RangedStore;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import java.util.Map;
import java.util.NavigableMap;
import lombok.RequiredArgsConstructor;

/**
 *
 *
 * @author Matt Laquidara
 */
@RequiredArgsConstructor
public class MapRangedStore<K extends RangedKey<K>, V> implements
        RangedStore<K, V> {

    private final NavigableMap<K, V> map;

    @Override
    public NavigableMap<K, V> getValuesInRange(final K bottom, final K top) {
        return map.subMap(bottom, true, top, true);
    }

    @Override
    public NavigableMap<K, V> getValuesAbove(final K bottom) {
        return getValuesInRange(bottom, bottom.getRangeMax());
    }

    @Override
    public NavigableMap<K, V> getValuesBelow(final K top) {
        return getValuesInRange(top.getRangeMin(), top);
    }

    @Override
    public boolean containsKey(final K key) {
        return map.containsKey(key);
    }

    @Override
    public V get(final K key) {
        return map.get(key);
    }

    @Override
    public void put(final K key, final V value) {
        map.put(key, value);
    }

    @Override
    public void delete(K key) {
        map.remove(key);
    }

    @Override
    public Multiset<V> getValues() {
        return ImmutableMultiset.copyOf(map.values());
    }

    @Override
    public boolean isEmpty() {
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
    public void putAll(final Map<K, V> values) {
        map.putAll(values);
    }

}
