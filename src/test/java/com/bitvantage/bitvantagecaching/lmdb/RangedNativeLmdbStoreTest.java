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
import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.GsonSerializer;
import com.bitvantage.bitvantagecaching.lmdb.RangedNativeLmdbStore;
import com.bitvantage.bitvantagecaching.testhelpers.KeyValueHelpers;
import com.bitvantage.bitvantagecaching.testhelpers.TestPartitionKey;
import com.bitvantage.bitvantagecaching.testhelpers.TestRangeKey;
import com.google.common.io.Files;
import java.io.File;
import java.nio.file.Path;
import java.util.SortedMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 *
 * @author Matt Laquidara
 */
public class RangedNativeLmdbStoreTest {

    @Test
    public void testGetsNothing() throws Exception {
        final RangedKeyManager<TestPartitionKey, TestRangeKey> keyManager
                = Mockito.mock(RangedKeyManager.class);
        final Serializer<String> serializer = Mockito.mock(Serializer.class);
        final RangedNativeLmdbStore<TestPartitionKey, TestRangeKey, String> store
                = getEmptyStore(keyManager, serializer);

        final TestPartitionKey partition = new TestPartitionKey("a");
        final TestRangeKey rangeMin = new TestRangeKey("a");
        final TestRangeKey rangeMax = new TestRangeKey("z");
        
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMin);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMax);
        KeyValueHelpers.mockNoValue(serializer);

        final SortedMap<TestRangeKey, String> output = store.getValuesInRange(
                partition, rangeMin, rangeMax);

        Assert.assertTrue(output.isEmpty());
    }

    @Test
    public void testGetsValueInRange() throws Exception {
        final RangedKeyManager<TestPartitionKey, TestRangeKey> keyManager
                = Mockito.mock(RangedKeyManager.class);
        final Serializer<String> serializer = Mockito.mock(Serializer.class);
        final RangedNativeLmdbStore<TestPartitionKey, TestRangeKey, String> store
                = getEmptyStore(keyManager, serializer);

        final TestPartitionKey partition = new TestPartitionKey("a");

        final TestRangeKey rangeMin = new TestRangeKey("a");
        final TestRangeKey rangeMax = new TestRangeKey("z");
        final TestRangeKey rangeValue = new TestRangeKey("b");

        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMin);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMax);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeValue);
        KeyValueHelpers.mockNoValue(serializer);

        store.put(partition, rangeValue, "");

        final SortedMap<TestRangeKey, String> output = store.getValuesInRange(
                partition, rangeMin, rangeMax);

        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testGetsValueBeginningOfRange() throws Exception {
        final RangedKeyManager<TestPartitionKey, TestRangeKey> keyManager
                = Mockito.mock(RangedKeyManager.class);
        final Serializer<String> serializer = Mockito.mock(Serializer.class);
        final RangedNativeLmdbStore<TestPartitionKey, TestRangeKey, String> store
                = getEmptyStore(keyManager, serializer);

        final TestPartitionKey partition = new TestPartitionKey("a");
        final TestRangeKey rangeMin = new TestRangeKey("a");
        final TestRangeKey rangeMax = new TestRangeKey("z");

        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMin);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMax);
        KeyValueHelpers.mockNoValue(serializer);

        store.put(partition, rangeMin, "");

        final SortedMap<TestRangeKey, String> output = store.getValuesInRange(
                partition, rangeMin, rangeMax);

        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testGetsValueEndOfRange() throws Exception {
        final RangedKeyManager<TestPartitionKey, TestRangeKey> keyManager
                = Mockito.mock(RangedKeyManager.class);
        final Serializer<String> serializer = Mockito.mock(Serializer.class);
        final RangedNativeLmdbStore<TestPartitionKey, TestRangeKey, String> store
                = getEmptyStore(keyManager, serializer);

        final TestPartitionKey partition = new TestPartitionKey("a");
        final TestRangeKey rangeMin = new TestRangeKey("a");
        final TestRangeKey rangeMax = new TestRangeKey("z");

        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMin);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMax);
        KeyValueHelpers.mockNoValue(serializer);

        store.put(partition, rangeMax, "");

        final SortedMap<TestRangeKey, String> output = store.getValuesInRange(
                partition, rangeMin, rangeMax);

        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testDoesNotGetValueBeyondEndOfRange() throws Exception {
        final RangedKeyManager<TestPartitionKey, TestRangeKey> keyManager
                = Mockito.mock(RangedKeyManager.class);
        final Serializer<String> serializer = Mockito.mock(Serializer.class);
        final RangedNativeLmdbStore<TestPartitionKey, TestRangeKey, String> store
                = getEmptyStore(keyManager, serializer);

        final TestPartitionKey partition = new TestPartitionKey("a");
        final TestRangeKey rangeMin = new TestRangeKey("a");
        final TestRangeKey rangeMax = new TestRangeKey("y");
        final TestRangeKey rangeValue = new TestRangeKey("z");

        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMin);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMax);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeValue);
        KeyValueHelpers.mockNoValue(serializer);

        store.put(partition, rangeValue, "");

        final SortedMap<TestRangeKey, String> output = store.getValuesInRange(
                partition, rangeMin, rangeMax);

        Assert.assertTrue(output.isEmpty());
    }

    @Test
    public void testDoesNotGetValueBeforeBeginningOfRange() throws Exception {
        final RangedKeyManager<TestPartitionKey, TestRangeKey> keyManager
                = Mockito.mock(RangedKeyManager.class);
        final Serializer<String> serializer = Mockito.mock(Serializer.class);
        final RangedNativeLmdbStore<TestPartitionKey, TestRangeKey, String> store
                = getEmptyStore(keyManager, serializer);

        final TestPartitionKey partition = new TestPartitionKey("a");
        final TestRangeKey rangeMin = new TestRangeKey("b");
        final TestRangeKey rangeMax = new TestRangeKey("z");
        final TestRangeKey rangeValue = new TestRangeKey("a");

        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMin);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMax);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeValue);
        KeyValueHelpers.mockNoValue(serializer);

        store.put(partition, rangeValue, "");

        final SortedMap<TestRangeKey, String> output = store.getValuesInRange(
                partition, rangeMin, rangeMax);

        Assert.assertTrue(output.isEmpty());
    }

    @Test
    public void testGetsAllValues() throws Exception {
        final RangedKeyManager<TestPartitionKey, TestRangeKey> keyManager
                = Mockito.mock(RangedKeyManager.class);
        final Serializer<String> serializer = Mockito.mock(Serializer.class);
        final RangedNativeLmdbStore<TestPartitionKey, TestRangeKey, String> store
                = getEmptyStore(keyManager, serializer);

        final TestPartitionKey partition = new TestPartitionKey("a");
        final TestRangeKey rangeMin = new TestRangeKey("b");
        final TestRangeKey rangeMax = new TestRangeKey("z");
        final TestRangeKey rangeValue = new TestRangeKey("m");

        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMin);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMax);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeValue);
        KeyValueHelpers.mockNoValue(serializer);

        store.put(partition, rangeMin, "");
        store.put(partition, rangeValue, "");
        store.put(partition, rangeMax, "");

        final SortedMap<TestRangeKey, String> output = store.getValuesInRange(
                partition, rangeMin, rangeMax);

        Assert.assertEquals(3, output.size());
    }

    @Test
    public void testGetsOnlyValues() throws Exception {
        final RangedKeyManager<TestPartitionKey, TestRangeKey> keyManager
                = Mockito.mock(RangedKeyManager.class);
        final Serializer<String> serializer = Mockito.mock(Serializer.class);
        final RangedNativeLmdbStore<TestPartitionKey, TestRangeKey, String> store
                = getEmptyStore(keyManager, serializer);

        final TestPartitionKey partition = new TestPartitionKey("a");
        final TestRangeKey rangeMin = new TestRangeKey("g");
        final TestRangeKey rangeMax = new TestRangeKey("z");
        final TestRangeKey outsideRange = new TestRangeKey("a");
        final TestRangeKey insideRange = new TestRangeKey("m");

        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMin);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMax);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, outsideRange);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, insideRange);
        KeyValueHelpers.mockNoValue(serializer);

        store.put(partition, outsideRange, "");
        store.put(partition, insideRange, "");
        store.put(partition, rangeMax, "");

        final SortedMap<TestRangeKey, String> output = store.getValuesInRange(
                partition, rangeMin, rangeMax);

        Assert.assertEquals(2, output.size());
    }

    @Test
    public void testGetsOnlyPartitionValues() throws Exception {

        final RangedKeyManager<TestPartitionKey, TestRangeKey> keyManager
                = Mockito.mock(RangedKeyManager.class);
        final Serializer<String> serializer = Mockito.mock(Serializer.class);

        final RangedNativeLmdbStore<TestPartitionKey, TestRangeKey, String> store
                = getEmptyStore(keyManager, serializer);

        final TestPartitionKey partition = new TestPartitionKey("a");
        final TestRangeKey rangeMin = new TestRangeKey("a");
        final TestRangeKey rangeMax = new TestRangeKey("z");
        final TestPartitionKey otherPartition = new TestPartitionKey("b");
        final TestRangeKey rangeValue = new TestRangeKey("m");

        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMin);
        KeyValueHelpers.mockKeyOperations(keyManager, otherPartition, rangeValue);
        KeyValueHelpers.mockKeyOperations(keyManager, partition, rangeMax);
        KeyValueHelpers.mockNoValue(serializer);

        store.put(partition, rangeMin, "");
        store.put(otherPartition, rangeValue, "");
        store.put(partition, rangeMax, "");

        final SortedMap<TestRangeKey, String> output = store.getValuesInRange(
                partition, rangeMin, rangeMax);

        Assert.assertEquals(2, output.size());
    }

    private RangedNativeLmdbStore<TestPartitionKey, TestRangeKey, String> getEmptyStore(
            final RangedKeyManager<TestPartitionKey, TestRangeKey> keyManager,
            final Serializer<String> serializer) {
        final File storeDir = Files.createTempDir();
        final Path path = storeDir.toPath();

        final RangedNativeLmdbStore<TestPartitionKey, TestRangeKey, String> store
                = new RangedNativeLmdbStore<>(path, keyManager, serializer, 1);

        return store;
    }


}
