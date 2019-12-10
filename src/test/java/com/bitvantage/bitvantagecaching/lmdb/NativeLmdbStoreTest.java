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

import com.bitvantage.bitvantagecaching.Serializer;
import com.bitvantage.bitvantagecaching.testhelpers.KeyValueHelpers;
import com.bitvantage.bitvantagecaching.testhelpers.TestPartitionKey;
import com.google.common.io.Files;
import java.io.File;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 *
 * @author Matt Laquidara
 */
public class NativeLmdbStoreTest {

    @Test
    public void testDoesNotContainKey() throws Exception {
        final KeyManager<TestPartitionKey> keyManager
                = Mockito.mock(KeyManager.class);
        final Serializer<String> serializer = Mockito.mock(Serializer.class);

        final NativeLmdbStore<TestPartitionKey, String> store = getEmptyStore(
                keyManager, serializer);
        
        final TestPartitionKey key = new TestPartitionKey("any");
        
        KeyValueHelpers.mockKeyOperations(keyManager, key);
        KeyValueHelpers.mockNoValue(serializer);
        
        Assert.assertFalse(store.containsKey(key));
    }

    @Test
    public void testContainsKey() throws Exception {
        final KeyManager<TestPartitionKey> keyManager
                = Mockito.mock(KeyManager.class);
        final Serializer<String> serializer = Mockito.mock(Serializer.class);

        final TestPartitionKey key = new TestPartitionKey("key");
        
        KeyValueHelpers.mockKeyOperations(keyManager, key);
        KeyValueHelpers.mockNoValue(serializer);

        final NativeLmdbStore<TestPartitionKey, String> store = getEmptyStore(
                keyManager, serializer);

        store.put(key, "");
        Assert.assertTrue(store.containsKey(key));
    }

    private NativeLmdbStore<TestPartitionKey, String> getEmptyStore(
            final KeyManager<TestPartitionKey> keyManager,
            final Serializer<String> serializer) {
        final File storeDir = Files.createTempDir();
        final Path path = storeDir.toPath();

        final NativeLmdbStore<TestPartitionKey, String> store
                = new NativeLmdbStore<>(
                        path, keyManager, serializer, 1);

        return store;
    }

}
