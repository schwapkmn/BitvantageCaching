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

import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.lmdbjava.Cursor;
import org.lmdbjava.GetOp;
import org.lmdbjava.Txn;

/**
 *
 * @author Public Transit Analytics
 */
public class RangedNativeLmdbStore<K extends RangedKey<K>, V>
        extends NativeLmdbStore<K, V> implements RangedStore<K, V> {

    public RangedNativeLmdbStore(final Path path, final Class valueType) {
        super(path, valueType);
    }

    @Override
    public List<V> getValuesInRange(final K min, final K max)
            throws InterruptedException {
        final ImmutableList.Builder<V> valuesBuilder = ImmutableList.builder();

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
                        valuesBuilder.add(getTypedValue(cursor.val()));
                    }
                    break;
                }
                valuesBuilder.add(getTypedValue(cursor.val()));
                present = cursor.next();
            }

            return valuesBuilder.build();
        }
    }

    @Override
    public List<V> getValuesAbove(K bottom) throws InterruptedException {
        return getValuesInRange(bottom, bottom.getRangeMax());
    }

    @Override
    public List<V> getValuesBelow(K top) throws InterruptedException {
        return getValuesInRange(top.getRangeMin(), top);
    }
}
