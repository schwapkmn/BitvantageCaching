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

/**
 *
 * @author Matt Laquidara
 */
public class RangedCachingStore<K extends RangedKey, V> extends CachingStore<K, V>
        implements RangedStore<K, V> {

    private final RangedStore store;

    public RangedCachingStore(final RangedStore<K, V> store,
                              final Cache<K, V> cache) {
        super(store, cache);
        this.store = store;
    }

    @Override
    public List<V> getValuesInRange(K bottom, K top)
            throws InterruptedException, BitvantageStoreException {
        return store.getValuesInRange(bottom, top);
    }

    @Override
    public List<V> getValuesAbove(K bottom)
            throws InterruptedException, BitvantageStoreException {
        return store.getValuesAbove(bottom);
    }

    @Override
    public List<V> getValuesBelow(K top)
            throws InterruptedException, BitvantageStoreException {
        return store.getValuesBelow(top);
    }

}
