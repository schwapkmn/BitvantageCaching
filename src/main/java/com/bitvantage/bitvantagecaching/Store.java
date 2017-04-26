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

import com.google.common.collect.Multiset;

/**
 * Interface for a key-value store.
 * 
 * @author Matt Laquidara
 */
public interface Store<K extends Key, V> {

    boolean containsKey(K key) throws InterruptedException;

    V get(K key) throws InterruptedException;

    void put(K key, V value) throws InterruptedException ;

    void delete(K key) throws InterruptedException;

    Multiset<V> getValues() throws InterruptedException;

    boolean isEmpty() throws InterruptedException;
}
