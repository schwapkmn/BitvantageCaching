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

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.gson.Gson;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

/**
 *
 * @author Public Transit Analytics
 */
public class NativeLmdbStore<K extends Key, V> implements Store<K, V> {

    protected final Env env;
    protected final Dbi<ByteBuffer> database;
    private final Gson serializer;
    private final Class valueType;

    public NativeLmdbStore(final Path path, final Class valueType) {
        env = Env.create().setMapSize(107374182400L).setMaxDbs(1).open(
                path.toFile());
        database = env.openDbi(path.toString(), DbiFlags.MDB_CREATE);
        serializer = new Gson();
        this.valueType = valueType;
    }

    @Override
    public boolean containsKey(final K key) throws InterruptedException {
        return get(key) != null;
    }

    @Override
    public V get(final K key) throws InterruptedException {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer valueBytes = database.get(txn, getKeyBytes(key));
            final V result = getTypedValue(valueBytes);
            return result;
        }
    }

    @Override
    public void put(final K key, final V value) throws InterruptedException {
        database.put(getKeyBytes(key), getValueBytes(value));
    }

    @Override
    public void delete(K key) throws InterruptedException {
        database.delete(getKeyBytes(key));
    }

    @Override
    public Multiset<V> getValues() throws InterruptedException {
        final ImmutableMultiset.Builder<V> valuesBuilder
                = ImmutableMultiset.builder();
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final Cursor<ByteBuffer> cursor = database.openCursor(txn);
            boolean present = cursor.first();
            while (present) {
                final ByteBuffer valueBytes = cursor.val();
                valuesBuilder.add(getTypedValue(valueBytes));
                present = cursor.next();
            }

            return valuesBuilder.build();
        }
    }

    @Override
    public boolean isEmpty() throws InterruptedException {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final Cursor<ByteBuffer> cursor = database.openCursor(txn);
            final boolean empty = !cursor.first();
            return empty;
        }
    }

    protected ByteBuffer getKeyBytes(final K key) {
        final byte[] bytes
                = key.getKeyString().getBytes(StandardCharsets.UTF_8);
        final ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes).flip();

        return buffer;
    }

    protected ByteBuffer getValueBytes(final V value) {
        final byte[] bytes
                = serializer.toJson(value).getBytes(StandardCharsets.UTF_8);       
        final ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes).flip();
                
        return buffer;
    }

    protected V getTypedValue(final ByteBuffer bytes) {
        final ByteBuffer newBuffer = ByteBuffer.allocate(bytes.capacity());
        newBuffer.put(bytes);
        
        final String jsonString = new String(newBuffer.array(),
                                             StandardCharsets.UTF_8);
        return (V) serializer.fromJson(jsonString, valueType);
    }

    @Override
    public int getMaxReaders() {
        return env.info().maxReaders;
    }

    @Override
    public void close() {
    }

}
