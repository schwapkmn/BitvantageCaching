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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author Matt Laquidara
 */
@RequiredArgsConstructor
public class TTLCache<K extends Key, V> implements Cache<K, V> {

    private final Store<K, TTLContainer<V>> store;
    private final Duration timeToLive;
    private final Clock clock;

    private int hits = 0;
    private int misses = 0;
    private int puts = 0;

    @Override
    public V get(K key) {
        TTLContainer<V> container = store.get(key);

        if (container == null) {
            System.err.println(String.format("Could not get key %s.", key));
        } else if (Instant.now(clock).isAfter(container.getExpiration())) {
           System.err.println(String.format("Key %s expired at %s.", key,
                                             container.getExpiration().toString()));
        } else {
            V value = container.getItem();
            //System.err.println(String.format("Found (%s %s).", key, value));
            hits++;
            return value;
        }
        misses++;
        return null;
    }

    @Override
    public void put(K key, V value) {
        TTLContainer<V> container = new TTLContainer(Instant.now(clock).plus(timeToLive), value);
        store.put(key, container);
        puts++;
    }

    @Override
    public void invalidate(K key) {
        TTLContainer<V> container = store.get(key);

        if (container != null) {
            store.put(key, new TTLContainer(Instant.now(), container.getItem()));
        }
    }

    @Override
    public String getStats() {
        return String.format("hits: %s; misses: %s; puts: %s", hits, misses, puts);
    }

}
