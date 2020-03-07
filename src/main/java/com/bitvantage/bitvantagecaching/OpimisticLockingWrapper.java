/*
 * Copyright 2020 Matt Laquidara.
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

import java.util.Optional;
import java.util.Random;
import java.util.UUID;

/**
 *
 * @author Matt Laquidara
 */
public class OpimisticLockingWrapper<K extends PartitionKey, V>
        implements OptimisticLockingStore<K, V> {

    final Store<K, VersionedWrapper<V>> store;
    final Random random;

    public OpimisticLockingWrapper(final Store<K, VersionedWrapper<V>> store) {
        this.store = store;
        random = new Random();
    }

    @Override
    public VersionedWrapper<V> get(final K key) throws BitvantageStoreException,
            InterruptedException {
        return store.get(key);
    }

    @Override
    public synchronized Optional<V> putOnMatch(
            final K key, final V value, final UUID match)
            throws BitvantageStoreException, InterruptedException {
        final VersionedWrapper<V> oldVersion = store.get(key);
        if (oldVersion.getVersion().equals(match)) {
            put(key, value);
            return Optional.of(oldVersion.getValue());
        }
        return Optional.empty();
    }

    @Override
    public void put(K key, V value) throws BitvantageStoreException,
            InterruptedException {
        store.put(key, new VersionedWrapper(UUID.randomUUID(), value));
    }

}
