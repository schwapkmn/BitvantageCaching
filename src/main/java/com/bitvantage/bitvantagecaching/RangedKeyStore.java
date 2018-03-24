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

/**
 *
 * @author Matt Laquidara
 */
public interface RangedKeyStore<K extends RangedKey<K>> {

    void close();

    boolean containsKey(final K key) throws BitvantageStoreException,
            InterruptedException;

    void delete(final K key) throws BitvantageStoreException,
            InterruptedException;

    int getMaxReaders();

    NavigableSet<K> getValuesAbove(final K min) throws InterruptedException,
            BitvantageStoreException;

    NavigableSet<K> getValuesBelow(final K max) throws InterruptedException,
            BitvantageStoreException;

    NavigableSet<K> getValuesInRange(final K min, final K max) throws
            InterruptedException, BitvantageStoreException;

    boolean isEmpty() throws BitvantageStoreException, InterruptedException;

    void put(final K key) throws BitvantageStoreException, InterruptedException;

    void putAll(final Set<K> entries);
    
}
