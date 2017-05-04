/*
 * Copyright 2017 Public Transit Analytics.
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author Public Transit Analytics
 */
public class ReaderControlledStore<K extends Key, V> implements Store<K, V> {

    private final Store<K, V> backingStore;
    protected final ExecutorService executor;

    public ReaderControlledStore(final Store<K, V> backingStore, 
                                 final int readers) {
        this.backingStore = backingStore;
        executor = Executors.newFixedThreadPool(Math.min(
                readers, backingStore.getMaxReaders()));
    }

    @Override
    public boolean containsKey(final K key) throws InterruptedException,
            BitvantageStoreException {
        final Callable<Boolean> call = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return backingStore.containsKey(key);
            }
        };
        try {
            return executor.submit(call).get();
        } catch (final ExecutionException e) {
            throw new BitvantageStoreException(e);
        }
    }

    @Override
    public V get(final K key) throws InterruptedException,
            BitvantageStoreException {
        final Callable<V> call = new Callable<V>() {
            @Override
            public V call() throws Exception {
                return backingStore.get(key);
            }
        };
        try {
            return executor.submit(call).get();
        } catch (final ExecutionException e) {
            throw new BitvantageStoreException(e);
        }
    }

    @Override
    public void put(final K key, final V value) throws InterruptedException,
            BitvantageStoreException {
        final Callable<Void> call = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                backingStore.put(key, value);
                return null;
            }
        };
        try {
            executor.submit(call).get();
        } catch (final ExecutionException e) {
            throw new BitvantageStoreException(e);
        }
    }

    @Override
    public void delete(K key) throws InterruptedException,
            BitvantageStoreException {
        final Callable<Void> call = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                backingStore.delete(key);
                return null;
            }
        };
        try {
            executor.submit(call).get();
        } catch (final ExecutionException e) {
            throw new BitvantageStoreException(e);
        }
    }

    @Override
    public Multiset<V> getValues() throws InterruptedException,
            BitvantageStoreException {
        final Callable<Multiset<V>> call = new Callable<Multiset<V>>() {
            @Override
            public Multiset<V> call() throws Exception {
                return backingStore.getValues();
            }
        };
        try {
            return executor.submit(call).get();
        } catch (final ExecutionException e) {
            throw new BitvantageStoreException(e);
        }
    }

    @Override
    public boolean isEmpty() throws InterruptedException,
            BitvantageStoreException {
        final Callable<Boolean> call = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return backingStore.isEmpty();
            }
        };
        try {
            return executor.submit(call).get();
        } catch (final ExecutionException e) {
            throw new BitvantageStoreException(e);
        }
    }

    @Override
    public int getMaxReaders() {
        return backingStore.getMaxReaders();
    }

    @Override
    public void close() {
        executor.shutdown();
    }

}
