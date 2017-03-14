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
import org.fusesource.lmdbjni.EntryIterator;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.Transaction;

/**
 *
 * @author Matt Laquidara
 */
public class LmdbStore<K extends Key, V> implements Store<K, V> {

    protected final Env env;
    private final Gson serializer;
    private final Class valueType;

    public LmdbStore(final Path path, final Class valueType) {
        env = new Env();
        env.setMapSize(107374182400L);
        env.open(path.toString());
        serializer = new Gson();
        this.valueType = valueType;
    }

    @Override
    public V get(K key) {
        final Database db = env.openDatabase();
        final byte[] keyBytes = getKeyBytes(key);
        final byte[] bytes = db.get(keyBytes);
        final V value;
        if (bytes == null) {
            value = null;
        } else {
            value = getValue(bytes);
        }
        db.close();

        return value;
    }

    @Override
    public void put(K key, V value) {
        final byte[] keyBytes = getKeyBytes(key);
        final byte[] valueBytes = getValueBytes(value);
        final Database db = env.openDatabase();
        db.put(keyBytes, valueBytes);
        db.close();
    }

    @Override
    public boolean isEmpty() {
        final Database db = env.openDatabase();
        final Transaction tx = env.createReadTransaction();
        final Cursor cursor = db.openCursor(tx);
        boolean empty = (null == cursor.get(Constants.FIRST));
        cursor.close();
        tx.commit();
        tx.close();
        db.close();
        return empty;
    }

    @Override
    public Multiset<V> getValues() {
        final Database db = env.openDatabase();
        final Transaction tx = env.createReadTransaction();
        final ImmutableMultiset.Builder<V> builder = ImmutableMultiset.builder();
        final Cursor cursor = db.openCursor(tx);
        try {
            for (Entry entry = cursor.get(Constants.FIRST); entry != null; entry
                 = cursor.
                 get(Constants.NEXT)) {
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

    public void copyFromBegining(Path newPath) {
        final Env newEnv = new Env();
        newEnv.setMapSize(107374182400L);
        newEnv.open(newPath.toString());

        final Database newDb = newEnv.openDatabase();

        final Database db = env.openDatabase();
        final Transaction tx = env.createReadTransaction();
        try {
            final Cursor cursor = db.openCursor(tx);
            try {
                for (Entry entry = cursor.get(Constants.FIRST); entry != null;
                     entry = cursor.
                     get(Constants.NEXT)) {
                    newDb.put(entry.getKey(), entry.getValue());
                }
            } finally {
                cursor.close();
            }

        } finally {
            tx.commit();
            tx.close();
            db.close();
            newDb.close();
            newEnv.close();
        }
    }

    public void copyFromEnd(Path newPath) {
        final Env newEnv = new Env();
        newEnv.setMapSize(107374182400L);
        newEnv.open(newPath.toString());

        final Database newDb = newEnv.openDatabase();

        final Database db = env.openDatabase();
        final Transaction tx = env.createReadTransaction();
        try {
            final EntryIterator itr = db.iterateBackward(tx);
            try {
                while (itr.hasNext()) {
                    Entry entry = itr.next();
                    newDb.put(entry.getKey(), entry.getValue());
                }
            } finally {
                itr.close();
            }

        } finally {
            tx.commit();
            tx.close();
            db.close();
            newDb.close();
            newEnv.close();
        }
    }

    public void close() {
        env.close();
    }

    @Override
    public boolean containsKey(K key) {
        final Database db = env.openDatabase();
        final boolean contains = db.get(getKeyBytes(key)) != null;
        db.close();

        return contains;
    }

    @Override
    public void delete(K key) {
        final Database db = env.openDatabase();
        db.delete(getKeyBytes(key));
        db.close();
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
}
