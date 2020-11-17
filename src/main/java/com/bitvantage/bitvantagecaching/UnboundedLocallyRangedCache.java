/*
 * Copyright 2020 Public Transit Analytics.
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

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author Public Transit Analytics
 */
@RequiredArgsConstructor
public class UnboundedLocallyRangedCache<P extends PartitionKey, R extends RangeKey<R>, V>
        implements RangedStore<P, R, V> {

    private final Store<P, Map<R, V>> store;
    private final Cache<P, UUID> cache;
    private final RangedStore<UuidKey, R, V> local;

    @Override
    public NavigableMap<R, V> getValuesInRange(
            final P partitionKey, final R min, final R max)
            throws InterruptedException, BitvantageStoreException {

        final CacheResult<P, UUID> cacheResult = cache.get(
                Collections.singleton(partitionKey));

        final UUID uuid;
        if (cacheResult.getCachedResults().isEmpty()) {
            final Map<R, V> partition = store.get(partitionKey);
            uuid = UUID.randomUUID();
            local.putAll(new UuidKey(uuid), partition);
            cache.put(Collections.singletonMap(partitionKey, uuid));
        } else {
            uuid = cacheResult.getCachedResults().values().iterator().next();
        }

        return local.getValuesInRange(new UuidKey(uuid), min, max);
    }

    @Override
    public NavigableMap<R, V> getValuesAbove(final P partition, final R min)
            throws InterruptedException, BitvantageStoreException {
        return getValuesInRange(partition, min, min.getRangeMax());
    }

    @Override
    public NavigableMap<R, V> getValuesBelow(final P partition, final R max)
            throws InterruptedException, BitvantageStoreException {
        return getValuesInRange(partition, max.getRangeMin(), max);
    }

    @Override
    public NavigableMap<R, V> getNextValues(P partition, R min, int count)
            throws InterruptedException, BitvantageStoreException {
        final UUID uuid = getPartitionUuid(partition);
        return local.getNextValues(new UuidKey(uuid), min, count);
    }

    @Override
    public NavigableMap<R, V> getHeadValues(P partition, int count) throws
            InterruptedException, BitvantageStoreException {
        final UUID uuid = getPartitionUuid(partition);
        return local.getHeadValues(new UuidKey(uuid), count);
    }

    @Override
    public NavigableMap<R, V> getPartition(P partition) throws
            InterruptedException, BitvantageStoreException {
        final UUID uuid = getPartitionUuid(partition);
        return local.getPartition(new UuidKey(uuid));
    }

    @Override
    public V get(P partition, R rangeValue) throws BitvantageStoreException,
            InterruptedException {
        final UUID uuid = getPartitionUuid(partition);
        return local.get(new UuidKey(uuid), rangeValue);
    }

    @Override
    public void put(P partition, R rangeValue, V value) throws
            BitvantageStoreException, InterruptedException {
        final Map<R, V> old = store.get(partition);
        store.put(partition, ImmutableMap.<R, V>builder().putAll(old)
                  .put(rangeValue, value).build());
    }

    @Override
    public void putAll(P partition, Map<R, V> entries) throws
            BitvantageStoreException, InterruptedException {
        store.put(partition, entries);
    }

    @Override
    public boolean isEmpty()
            throws BitvantageStoreException, InterruptedException {
        return store.isEmpty();
    }

    private UUID getPartitionUuid(final P partitionKey)
            throws BitvantageStoreException, InterruptedException {
        final CacheResult<P, UUID> cacheResult = cache.get(
                Collections.singleton(partitionKey));

        final UUID uuid;
        if (cacheResult.getCachedResults().isEmpty()) {
            final Map<R, V> partition = store.get(partitionKey);
            uuid = UUID.randomUUID();
            local.putAll(new UuidKey(uuid), partition);
            cache.put(Collections.singletonMap(partitionKey, uuid));
        } else {
            uuid = cacheResult.getCachedResults().values().iterator().next();
        }
        return uuid;
    }

}
