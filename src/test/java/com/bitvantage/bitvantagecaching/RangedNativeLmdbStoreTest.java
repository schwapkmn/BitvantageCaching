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
package com.bitvantage.bitvantagecaching;

import com.bitvantage.bitvantagecaching.testhelpers.TestRangedKey;
import com.google.common.io.Files;
import java.io.File;
import java.nio.file.Path;
import java.util.SortedMap;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Matt Laquidara
 */
public class RangedNativeLmdbStoreTest {

    @Test
    public void testGetsNothing() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = getEmptyStore();

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'z'));
        store.close();
        Assert.assertTrue(output.isEmpty());
    }

    @Test
    public void testGetsValueInRange() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = getEmptyStore();
        store.put(new TestRangedKey('a', 'b'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testGetsValueBeginningOfRange() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testGetsValueEndOfRange() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = getEmptyStore();
        store.put(new TestRangedKey('a', 'z'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testDoesNotGetValueBeyondEndOfRange() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = getEmptyStore();
        store.put(new TestRangedKey('a', 'z'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'y'));

        store.close();
        Assert.assertTrue(output.isEmpty());
    }

    @Test
    public void testDoesNotGetValueBeforeBeginningOfRange() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'b'), new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertTrue(output.isEmpty());
    }

    @Test
    public void testGetsAllValues() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");
        store.put(new TestRangedKey('a', 'm'), "");
        store.put(new TestRangedKey('a', 'z'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertEquals(3, output.size());
    }

    @Test
    public void testGetsOnlyValues() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");
        store.put(new TestRangedKey('a', 'm'), "");
        store.put(new TestRangedKey('a', 'z'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'g'), new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertEquals(2, output.size());
    }

    @Test
    public void testGetsOnlyRangedValues() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");
        store.put(new TestRangedKey('b', 'm'), "");
        store.put(new TestRangedKey('a', 'z'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertEquals(2, output.size());
    }

    private RangedNativeLmdbStore<TestRangedKey, String> getEmptyStore() {
        final File storeDir = Files.createTempDir();
        final Path path = storeDir.toPath();

        final RangedNativeLmdbStore<TestRangedKey, String> store
                = new RangedNativeLmdbStore<>(
                        path, new TestRangedKey.Materializer(),
                        new GsonSerializer(String.class), 1);

        return store;
    }

}
