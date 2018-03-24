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
package com.bitvantage.bitvantagecaching.mocks;

import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.RangedKey;
import com.bitvantage.bitvantagecaching.RangedKeyStore;
import java.util.NavigableSet;
import java.util.Set;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author Matt Laquidara
 */
@RequiredArgsConstructor
public class SetRangedKeyStore<K extends RangedKey<K>>
        implements RangedKeyStore<K> {

    private final NavigableSet<K> set;

    @Override
    public void close() {
    }

    @Override
    public boolean containsKey(K key) throws BitvantageStoreException,
            InterruptedException {
        return set.contains(key);
    }

    @Override
    public void delete(K key) throws BitvantageStoreException,
            InterruptedException {
        set.remove(key);
    }

    @Override
    public int getMaxReaders() {
        return Integer.MAX_VALUE;
    }

    @Override
    public NavigableSet<K> getValuesAbove(K min) throws InterruptedException,
            BitvantageStoreException {
        return set.tailSet(min, true);
    }

    @Override
    public NavigableSet<K> getValuesBelow(K max) throws InterruptedException,
            BitvantageStoreException {
        return set.headSet(max, true);
    }

    @Override
    public NavigableSet<K> getValuesInRange(K min, K max) throws
            InterruptedException, BitvantageStoreException {
        return set.subSet(min, true, max, true);
    }

    @Override
    public boolean isEmpty() throws BitvantageStoreException,
            InterruptedException {
        return set.isEmpty();
    }

    @Override
    public void put(K key) throws BitvantageStoreException,
            InterruptedException {
        set.add(key);
    }

    @Override
    public void putAll(Set<K> entries) {
        set.addAll(entries);
    }

}
