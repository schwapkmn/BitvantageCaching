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

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.fusesource.lmdbjni.Constants;
import org.fusesource.lmdbjni.Cursor;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;

/**
 *
 * @author Matt Laquidara
 */
public class LmdbStore<K extends Key, V> implements Store<K, V> {

    protected final Env env;
    protected final Database db;
    private final Path path;
    private final Gson serializer;
    private final Class valueType;

    public LmdbStore(final Path path, final Class valueType) {
        env = new Env();
        env.setMapSize(107374182400L);
        env.open(path.toString());
        env.readerCheck();
        serializer = new Gson();
        db = env.openDatabase();

        this.valueType = valueType;
        this.path = path;
    }

    @Override
    public V get(K key) throws InterruptedException {
        try {
            final byte[] keyBytes = getKeyBytes(key);
            final byte[] bytes = db.get(keyBytes);
            final V value;
            if (bytes == null) {
                value = null;
            } else {
                value = getValue(bytes);
            }
            return value;
        } finally {
        }
    }

    @Override
    public void put(K key, V value) throws InterruptedException {
        final byte[] keyBytes = getKeyBytes(key);
        final byte[] valueBytes = getValueBytes(value);
        try {
            db.put(keyBytes, valueBytes);
        } finally {
        }
    }

    @Override
    public boolean isEmpty() throws InterruptedException {
        final Database db = env.openDatabase();
        final Transaction tx = env.createReadTransaction();
        final Cursor cursor = db.openCursor(tx);

        try {
            boolean empty = (null == cursor.get(Constants.FIRST));
            return empty;
        } finally {
            cursor.close();
            tx.commit();
            tx.close();
            db.close();
        }
    }

    @Override
    public Multiset<V> getValues() throws InterruptedException {
        final Transaction tx = env.createReadTransaction();
        final ImmutableMultiset.Builder<V> builder = ImmutableMultiset
                .builder();
        final Cursor cursor = db.openCursor(tx);
        try {
            for (Entry entry = cursor.get(Constants.FIRST); entry != null;
                 entry = cursor.get(Constants.NEXT)) {
                builder.add(getValue(entry.getValue()));
            }
            return builder.build();
        } finally {
            tx.commit();
            tx.close();
            db.close();
            cursor.close();
        }
    }

    @Override
    public void close() {
        db.close();
        env.close();
    }

    @Override
    public boolean containsKey(K key) throws InterruptedException {
        return get(key) == null;
    }

    @Override
    public void delete(K key) throws InterruptedException {
        db.delete(getKeyBytes(key));
    }

    protected byte[] getKeyBytes(final K key) {
        return key.getKeyString().getBytes(StandardCharsets.UTF_8);
    }

    protected byte[] getValueBytes(final V value) {
        return serializer.toJson(value).getBytes(StandardCharsets.UTF_8);
    }

    protected V getValue(byte[] bytes) {
        final String jsonString = new String(bytes, StandardCharsets.UTF_8);
        return (V) serializer.fromJson(jsonString, valueType);
    }

    @Override
    public int getMaxReaders() {
        return (int) env.getMaxReaders();
    }

}
