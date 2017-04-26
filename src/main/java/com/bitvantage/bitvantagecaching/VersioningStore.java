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

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author Matt Laquidara
 */
@RequiredArgsConstructor
public class VersioningStore<K extends Key, V> implements Store<K, V> {

    private final Store<K, List<VersionedValue<V>>> store;
    private final Clock clock;

    @Override
    public V get(K key) throws InterruptedException {
        List<VersionedValue<V>> values = store.get(key);

        if (values == null) {
            return null;
        }
        return getLatest(values);
    }

    @Override
    public void put(K key, V newValue) throws InterruptedException {
        List<VersionedValue<V>> existingValue = store.get(key);

        List<VersionedValue<V>> value;
        if (existingValue == null) {
            value = new ArrayList<>(1);
        } else {
            value = new ArrayList<>(existingValue);
        }

        value.add(new VersionedValue(Instant.now(clock), newValue));
        store.put(key, value);
    }

    @Override
    public boolean isEmpty() throws InterruptedException {
        return store.isEmpty();
    }

    @Override
    public boolean containsKey(K key) throws InterruptedException {
        return store.containsKey(key);
    }

    @Override
    public void delete(K key) {
        /* TODO: Put a sentinal value at the head of the keys */
    }

    @Override
    public Multiset<V> getValues() throws InterruptedException {
        final ImmutableMultiset.Builder<V> builder = ImmutableMultiset.builder();
        final Multiset<List<VersionedValue<V>>> versionedValues = store.getValues();
        for (List<VersionedValue<V>> versionedValue : versionedValues) {
            builder.add(getLatest(versionedValue));
        }
        return builder.build();
    }

    private V getLatest(List<VersionedValue<V>> versionedValue) {
        System.err.println(String.format("Got %s values", versionedValue.size()));

        final TreeMap<Instant, V> map = new TreeMap<>();
        for (VersionedValue<V> value : versionedValue) {
            map.put(value.getVersion(), value.getValue());
        }
        return map.lastEntry().getValue();
    }
}
