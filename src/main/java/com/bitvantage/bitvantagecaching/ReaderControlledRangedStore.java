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

import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Matt Laquidara
 */
public class ReaderControlledRangedStore<K extends RangedKey, V> extends ReaderControlledStore<K, V>
        implements RangedStore<K, V> {

    private final RangedStore<K, V> backingStore;

    public ReaderControlledRangedStore(final RangedStore<K, V> backingStore,
                                       final int readers) {
        super(backingStore, readers);
        this.backingStore = backingStore;
    }

    @Override
    public SortedMap<K, V> getValuesInRange(final K min, final K max) throws
            InterruptedException, BitvantageStoreException {
        final Callable<SortedMap<K, V>> call = new Callable<SortedMap<K, V>>() {
            @Override
            public SortedMap<K, V> call() throws Exception {
                return backingStore.getValuesInRange(min, max);
            }
        };
        try {
            return executor.submit(call).get();
        } catch (final ExecutionException e) {
            throw new BitvantageStoreException(e);
        }
    }

    @Override
    public SortedMap<K, V> getValuesAbove(final K min) throws
            InterruptedException, BitvantageStoreException {
        final Callable<SortedMap<K, V>> call = new Callable<SortedMap<K, V>>() {
            @Override
            public SortedMap<K, V> call() throws Exception {
                return backingStore.getValuesAbove(min);
            }
        };
        try {
            return executor.submit(call).get();
        } catch (final ExecutionException e) {
            throw new BitvantageStoreException(e);
        }
    }

    @Override
    public SortedMap<K, V> getValuesBelow(K max) throws InterruptedException,
            BitvantageStoreException {
        final Callable<SortedMap<K, V>> call = new Callable<SortedMap<K, V>>() {
            @Override
            public SortedMap<K, V> call() throws Exception {
                return backingStore.getValuesBelow(max);
            }
        };
        try {
            return executor.submit(call).get();
        } catch (final ExecutionException e) {
            throw new BitvantageStoreException(e);
        }
    }

    @Override
    public void putRange(SortedMap<K, V> values)  throws InterruptedException,
            BitvantageStoreException {
        final Callable<Void> call = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                backingStore.putRange(values);
                return null;
            }
        };
        try {
            executor.submit(call).get();
        } catch (final ExecutionException e) {
            throw new BitvantageStoreException(e);
        }
    }

}
