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

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.SortedMap;
import org.lmdbjava.Cursor;
import org.lmdbjava.GetOp;
import org.lmdbjava.Txn;

/**
 *
 * @author Public Transit Analytics
 */
public class RangedNativeLmdbStore<K extends RangedKey<K>, V>
        extends NativeLmdbStore<K, V> implements RangedStore<K, V> {

    private final KeyMaterializer<K> keyMaterializer;

    public RangedNativeLmdbStore(final Path path,
                                 final KeyMaterializer keyMaterializer,
                                 final Class valueType) {
        super(path, valueType);
        this.keyMaterializer = keyMaterializer;
    }

    @Override
    public SortedMap<K, V> getValuesInRange(final K min, final K max)
            throws InterruptedException, BitvantageStoreException {
        final ImmutableSortedMap.Builder<K, V> valuesBuilder
                = new ImmutableSortedMap.Builder<>(Ordering.natural());

        try (Txn<ByteBuffer> txn = env.txnRead()) {

            final boolean addLast;
            final Cursor<ByteBuffer> maxKeyCursor = database.openCursor(txn);
            final ByteBuffer maxKeyBytes = getKeyBytes(max);
            boolean keysBeyondMax
                    = maxKeyCursor.get(maxKeyBytes, GetOp.MDB_SET_RANGE);

            final ByteBuffer lastKeyBytes;
            if (keysBeyondMax) {
                lastKeyBytes = maxKeyCursor.key();
                if (lastKeyBytes.equals(maxKeyBytes)) {
                    addLast = true;
                } else {
                    addLast = false;
                }
            } else {
                lastKeyBytes = null;
                addLast = false;
            }

            final Cursor<ByteBuffer> cursor = database.openCursor(txn);
            boolean present = cursor.get(getKeyBytes(min), GetOp.MDB_SET_RANGE);

            while (present) {
                final ByteBuffer keyBytes = cursor.key();
                if (lastKeyBytes != null && lastKeyBytes.equals(keyBytes)) {
                    if (addLast) {
                        valuesBuilder.put(getTypedKey(cursor.key()),
                                          getTypedValue(cursor.val())
                        );
                    }
                    break;
                }
                valuesBuilder.put(getTypedKey(cursor.key()),
                                  getTypedValue(cursor.val()));
                present = cursor.next();
            }

            return valuesBuilder.build();
        }
    }

    @Override
    public void putRange(SortedMap<K, V> values) {
        final Txn<ByteBuffer> txn = env.txnWrite();
        try {
            for (Map.Entry<K, V> entry : values.entrySet()) {
                database.put(txn, getKeyBytes(entry.getKey()),
                             getValueBytes(entry.getValue()));
            }
        } finally {
            txn.commit();
            txn.close();
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

    private K getTypedKey(final ByteBuffer bytes) 
            throws BitvantageStoreException {
        final ByteBuffer newBuffer = ByteBuffer.allocate(bytes.capacity());
        newBuffer.put(bytes);
        final String keyString = new String(newBuffer.array(),
                                            StandardCharsets.UTF_8);
        return keyMaterializer.materialize(keyString);
    }

}
