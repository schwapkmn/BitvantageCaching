/*
 * Copyright 2018 Matt Laquidara.
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

import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author Matt Laquidara
 */
@RequiredArgsConstructor
public class StoreBackedRangedKeyStore<K extends RangedKey<K>> implements RangedKeyStore<K> {

    private final RangedStore<K, Byte> store;

    @Override
    public NavigableSet<K> getValuesInRange(final K min, final K max)
            throws InterruptedException, BitvantageStoreException {
        return store.getValuesInRange(min, max).navigableKeySet();
    }

    @Override
    public NavigableSet<K> getValuesAbove(final K min)
            throws InterruptedException, BitvantageStoreException {
        return store.getValuesAbove(min).navigableKeySet();
    }

    @Override
    public NavigableSet<K> getValuesBelow(final K max)
            throws InterruptedException, BitvantageStoreException {
        return store.getValuesBelow(max).navigableKeySet();
    }

    @Override
    public boolean containsKey(final K key) throws BitvantageStoreException,
            InterruptedException {
        return store.containsKey(key);
    }

    @Override
    public void put(final K key) throws BitvantageStoreException,
            InterruptedException {
        store.put(key, (byte) 0);
    }

    @Override
    public void putAll(final Set<K> entries) {
        store.putAll(entries.stream().collect(
                Collectors.toMap(Function.identity(), entry -> (byte) 0)));

    }

    @Override
    public void delete(final K key) throws BitvantageStoreException,
            InterruptedException {
        store.delete(key);
    }

    @Override
    public boolean isEmpty() throws BitvantageStoreException,
            InterruptedException {
        return store.isEmpty();
    }

    @Override
    public int getMaxReaders() {
        return store.getMaxReaders();
    }

    @Override
    public void close() {
        store.close();
    }

}
