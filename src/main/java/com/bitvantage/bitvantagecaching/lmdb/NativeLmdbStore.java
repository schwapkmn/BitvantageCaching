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
package com.bitvantage.bitvantagecaching.lmdb;

import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.Store;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.google.common.collect.ImmutableMap;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import com.bitvantage.bitvantagecaching.ValueSerializer;

/**
 *
 * @author Matt Laquidara
 */
public class NativeLmdbStore<K extends PartitionKey, V> implements Store<K, V> {

    private final Env<ByteBuffer> env;
    private final Dbi<ByteBuffer> db;
    private final KeyManager<K> keyManager;
    private final ValueSerializer<V> serializer;

    public NativeLmdbStore(final Path path, final KeyManager<K> keyManager,
                           final ValueSerializer<V> serializer, final int readers) {
        env = Env.create().setMaxDbs(1).setMapSize(107374182400L)
                .setMaxReaders(readers).open(path.toFile());
        db = env.openDbi("DB", DbiFlags.MDB_CREATE);
        this.keyManager = keyManager;
        this.serializer = serializer;
    }

    @Override
    public V get(final K key) 
            throws InterruptedException, BitvantageStoreException {
        final Txn tx = env.txnRead();
        try {
            final ByteBuffer keyBytes = getKeyBytes(key);

            final ByteBuffer bytes = db.get(tx, keyBytes);
            final V value;
            if (bytes == null) {
                value = null;
            } else {
                value = getValue(bytes);
            }
            return value;
        } finally {
            tx.close();
        }
    }

    @Override
    public void put(final K key, final V value)
            throws InterruptedException, BitvantageStoreException {
        final ByteBuffer keyBytes = getKeyBytes(key);
        final ByteBuffer valueBytes = getValueBytes(value);
        try {
            db.put(keyBytes, valueBytes);
        } finally {
        }
    }

    @Override
    public void putAll(final Map<K, V> entries)
            throws BitvantageStoreException {
        final Txn tx = env.txnWrite();
        try {
            for (Map.Entry<K, V> entry : entries.entrySet()) {
                db.put(tx, getKeyBytes(entry.getKey()),
                       getValueBytes(entry.getValue()));
            }
        } finally {
            tx.commit();
            tx.close();
        }
    }

    @Override
    public boolean isEmpty() throws InterruptedException {
        final Txn tx = env.txnRead();
        final Cursor cursor = db.openCursor(tx);

        try {
            boolean empty = !cursor.first();
            return empty;
        } finally {
            cursor.close();
            tx.commit();
            tx.close();
        }
    }

    @Override
    public Map<K, V> getAll() throws InterruptedException, 
            BitvantageStoreException {
        final Txn tx = env.txnRead();
        final ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        final Cursor<ByteBuffer> cursor = db.openCursor(tx);
        try {
            boolean hasNext = cursor.first();

            while (hasNext) {
                builder.put(getKey(cursor.key()), getValue(cursor.val()));
                hasNext = cursor.next();
            }
            return builder.build();
        } finally {
            tx.commit();
            tx.close();
            cursor.close();
        }
    }

    @Override
    public boolean containsKey(K key) 
            throws InterruptedException, BitvantageStoreException {
        return get(key) != null;
    }

    private ByteBuffer getKeyBytes(final K key) {
        final byte[] byteArray = keyManager.createKeyString(key).getBytes(
                StandardCharsets.UTF_8);

        final ByteBuffer buffer = ByteBuffer.allocateDirect(byteArray.length);
        buffer.put(byteArray).flip();
        return buffer;

    }

    private ByteBuffer getValueBytes(final V value) 
            throws BitvantageStoreException {
        final byte[] byteArray = serializer.getBytes(value);

        final ByteBuffer buffer = ByteBuffer.allocateDirect(byteArray.length);
        buffer.put(byteArray).flip();
        return buffer;
    }

    private V getValue(final ByteBuffer bytes) throws BitvantageStoreException {
        final byte[] byteArray = new byte[bytes.capacity()];
        bytes.get(byteArray, 0, bytes.capacity());
        return serializer.getValue(byteArray);
    }

    private K getKey(final ByteBuffer bytes) throws BitvantageStoreException {
        final byte[] byteArray = new byte[bytes.capacity()];
        final String keyText = new String(byteArray, StandardCharsets.UTF_8);
        return keyManager.materialize(keyText);
    }

}
