/*
 * Copyright 2018 Public Transit Analytics.
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NavigableMap;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.KeyRange;
import org.lmdbjava.KeyRangeType;
import org.lmdbjava.Txn;

/**
 * Uses LMDB to associate ranged keys with values.
 *
 * @author Matt Laquidara
 */
public class RangedNativeLmdbStore<K extends RangedKey<K>, V>
        extends NativeLmdbStore<K, V> implements RangedStore<K, V> {

    private final KeyMaterializer<K> keyMaterializer;

    public RangedNativeLmdbStore(final Path path,
                                 final KeyMaterializer<K> keyMaterializer,
                                 final Serializer<V> serializer,
                                 final int readers) {
        super(path, serializer, readers);
        this.keyMaterializer = keyMaterializer;
    }

    @Override
    public NavigableMap<K, V> getValuesInRange(final K min, final K max)
            throws InterruptedException, BitvantageStoreException {
        final Txn<ByteBuffer> tx = env.txnRead();
        final ByteBuffer maxKeyBytes = getKeyBytes(max);
        final ByteBuffer minKeyBytes = getKeyBytes(min);
        final KeyRange range = new KeyRange(KeyRangeType.FORWARD_CLOSED,
                                            minKeyBytes, maxKeyBytes);

        final ImmutableSortedMap.Builder<K, V> builder
                = ImmutableSortedMap.naturalOrder();
        final CursorIterator<ByteBuffer> iterator = db.iterate(tx, range);

        try {

            while (iterator.hasNext()) {
                final CursorIterator.KeyVal<ByteBuffer> keyValue
                        = iterator.next();
                builder.put(getKey(keyValue.key()),
                            getValue(keyValue.val()));
            }
            return builder.build();
        } finally {
            iterator.close();
            tx.commit();
            tx.close();
        }
    }

    @Override
    public NavigableMap<K, V> getValuesAbove(K bottom)
            throws InterruptedException, BitvantageStoreException {
        return getValuesInRange(bottom, bottom.getRangeMax());
    }

    @Override
    public NavigableMap<K, V> getValuesBelow(K top)
            throws InterruptedException, BitvantageStoreException {
        return getValuesInRange(top.getRangeMin(), top);
    }

    private K getKey(final ByteBuffer bytes) throws BitvantageStoreException {
        final byte[] byteArray = new byte[bytes.capacity()];
        bytes.get(byteArray, 0, bytes.capacity());
        final String keyString = new String(byteArray, StandardCharsets.UTF_8);
        return keyMaterializer.materialize(keyString);
    }

}
