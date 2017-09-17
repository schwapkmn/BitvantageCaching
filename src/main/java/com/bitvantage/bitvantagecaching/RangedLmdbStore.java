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

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.EntryIterator;
import org.fusesource.lmdbjni.Transaction;

/**
 * Uses LMDB to associate ranged keys with values.
 *
 * @author Matt Laquidara
 */
public class RangedLmdbStore<K extends RangedKey<K>, V> extends LmdbStore<K, V>
        implements RangedStore<K, V> {

    private final KeyMaterializer<K> keyMaterializer;

    public RangedLmdbStore(final Path path,
                           final KeyMaterializer<K> keyMaterializer,
                           final Class<V> valueType) {
        super(path, valueType);
        this.keyMaterializer = keyMaterializer;
    }

    @Override
    public SortedMap<K, V> getValuesInRange(final K min, final K max)
            throws InterruptedException, BitvantageStoreException {
        final Transaction tx = env.createReadTransaction();

        final byte[] maxKeyBytes = getKeyBytes(max);
        final EntryIterator maxIterator = db.seek(tx, maxKeyBytes);

        final byte[] minKeyBytes = getKeyBytes(min);
        final EntryIterator forwardIterator = db.seek(tx, minKeyBytes);

        final ImmutableSortedMap.Builder<K, V> builder
                = new ImmutableSortedMap.Builder<>(Ordering.natural());

        try {
            boolean addLast = false;
            final byte[] lastKeyBytes;
            /* If the max iterator does not have a next item, our max key is
             * beyond the end of the datastore. Thus search until the end. */
            if (maxIterator.hasNext()) {
                final Entry lastEntry = maxIterator.next();
                lastKeyBytes = lastEntry.getKey();

                /* If lastKey matches maxKey, include its value in the output. */
                if (Arrays.equals(lastKeyBytes, maxKeyBytes)) {
                    addLast = true;
                }
            } else {
                lastKeyBytes = null;
            }

            for (final Entry entry : forwardIterator.iterable()) {
                final byte[] key = entry.getKey();

                if (lastKeyBytes != null && Arrays.equals(lastKeyBytes, key)) {
                    if (addLast) {
                        builder.put(getKey(entry.getKey()),
                                    getValue(entry.getValue()));
                    }
                    break;
                }
                builder.put(getKey(entry.getKey()), getValue(entry.getValue()));
            }
            return builder.build();
        } finally {
            forwardIterator.close();
            maxIterator.close();
            tx.commit();
            tx.close();
        }
    }

    @Override
    public SortedMap<K, V> getValuesAbove(K bottom) 
            throws InterruptedException, BitvantageStoreException {
        return getValuesInRange(bottom, bottom.getRangeMax());
    }

    @Override
    public SortedMap<K, V> getValuesBelow(K top) 
            throws InterruptedException, BitvantageStoreException {
        return getValuesInRange(top.getRangeMin(), top);
    }

    @Override
    public void putRange(SortedMap<K, V> values) {
        final Transaction tx = env.createWriteTransaction();
        try {
            for (Map.Entry<K, V> entry : values.entrySet()) {
                db.put(tx, getKeyBytes(entry.getKey()),
                       getValueBytes(entry.getValue()));
            }
        } finally {
            tx.commit();
            tx.close();
        }
    }

    private K getKey(final byte[] bytes) throws BitvantageStoreException {
        final String keyString = new String(bytes, StandardCharsets.UTF_8);
        return keyMaterializer.materialize(keyString);
    }

}
