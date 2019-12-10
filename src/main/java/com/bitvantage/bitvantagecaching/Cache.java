/*
 * Copyright 2019 Matt Laquidara.
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
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author Matt Laquidara
 */
@RequiredArgsConstructor
public class Cache<P extends PartitionKey, V> {
    
    private final Store<P, V> store;
    
    public CacheResult<P, V> get(final Set<P> keys)
            throws BitvantageStoreException, InterruptedException {
        final ImmutableMap.Builder<P, V> cachedBuilder = ImmutableMap.builder();
        final ImmutableSet.Builder<P> uncachedBuilder = ImmutableSet.builder();
        
        for (final P key : keys) {
            final V value = store.get(key);
            if (value == null) {
                uncachedBuilder.add(key);
            } else {
                cachedBuilder.put(key, value);
            }
        }
        
        return new CacheResult(cachedBuilder.build(), uncachedBuilder.build());
    }
    
    public void put(final Map<P, V> contents) throws BitvantageStoreException,
            InterruptedException {
        store.putAll(contents);
    }
    
}
