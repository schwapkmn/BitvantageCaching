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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 *
 * @author Public Transit Analytics
 */
public class ReaderControlledRangedStore<K extends RangedKey, V> extends ReaderControlledStore<K, V>
        implements RangedStore<K, V> {

    private RangedStore<K, V> backingStore;

    public ReaderControlledRangedStore(final RangedStore<K, V> backingStore,
                                       final int readers) {
        super(backingStore, readers);
        this.backingStore = backingStore;
    }

    @Override
    public List<V> getValuesInRange(final K min, final K max) throws
            InterruptedException, BitvantageStoreException {
        final Callable<List<V>> call = new Callable<List<V>>() {
            @Override
            public List<V> call() throws Exception {
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
    public List<V> getValuesAbove(final K min) throws InterruptedException,
            BitvantageStoreException {
        final Callable<List<V>> call = new Callable<List<V>>() {
            @Override
            public List<V> call() throws Exception {
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
    public List<V> getValuesBelow(K max) throws InterruptedException,
            BitvantageStoreException {
        final Callable<List<V>> call = new Callable<List<V>>() {
            @Override
            public List<V> call() throws Exception {
                return backingStore.getValuesBelow(max);
            }
        };
        try {
            return executor.submit(call).get();
        } catch (final ExecutionException e) {
            throw new BitvantageStoreException(e);
        }
    }

}
