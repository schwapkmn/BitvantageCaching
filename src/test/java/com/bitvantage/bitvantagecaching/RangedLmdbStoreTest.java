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

import com.bitvantage.bitvantagecaching.testhelpers.TestRangedKey;
import com.google.common.io.Files;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Matt Laquidara
 */
public class RangedLmdbStoreTest {

    public RangedLmdbStoreTest() {

    }

    @Test
    public void testGetsNothing() {
        final RangedLmdbStore<TestRangedKey, String> store = getEmptyStore();

        final List<String> output = store.getValuesInRange(new TestRangedKey('a', 'a'),
                                                           new TestRangedKey('a', 'z'));
        store.close();
        Assert.assertTrue(output.isEmpty());
    }

    @Test
    public void testGetsValueInRange() {
        final RangedLmdbStore<TestRangedKey, String> store = getEmptyStore();
        store.put(new TestRangedKey('a', 'b'), "");

        final List<String> output = store.getValuesInRange(new TestRangedKey('a', 'a'),
                                                           new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testGetsValueBeginningOfRange() {
        final RangedLmdbStore<TestRangedKey, String> store = getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");

        final List<String> output = store.getValuesInRange(new TestRangedKey('a', 'a'),
                                                           new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testGetsValueEndOfRange() {
        final RangedLmdbStore<TestRangedKey, String> store = getEmptyStore();
        store.put(new TestRangedKey('a', 'z'), "");

        final List<String> output = store.getValuesInRange(new TestRangedKey('a', 'a'),
                                                           new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testDoesNotGetValueBeyondEndOfRange() {
        final RangedLmdbStore<TestRangedKey, String> store = getEmptyStore();
        store.put(new TestRangedKey('a', 'z'), "");

        final List<String> output = store.getValuesInRange(new TestRangedKey('a', 'a'),
                                                           new TestRangedKey('a', 'y'));

        store.close();
        Assert.assertTrue(output.isEmpty());
    }

    @Test
    public void testDoesNotGetValueBeforeBeginningOfRange() {
        final RangedLmdbStore<TestRangedKey, String> store = getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");

        final List<String> output = store.getValuesInRange(new TestRangedKey('a', 'b'),
                                                           new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertTrue(output.isEmpty());
    }

    @Test
    public void testGetsAllValues() {
        final RangedLmdbStore<TestRangedKey, String> store = getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");
        store.put(new TestRangedKey('a', 'm'), "");
        store.put(new TestRangedKey('a', 'z'), "");

        final List<String> output = store.getValuesInRange(new TestRangedKey('a', 'a'),
                                                           new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertEquals(3, output.size());
    }

    @Test
    public void testGetsOnlyValues() {
        final RangedLmdbStore<TestRangedKey, String> store = getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");
        store.put(new TestRangedKey('a', 'm'), "");
        store.put(new TestRangedKey('a', 'z'), "");

        final List<String> output = store.getValuesInRange(new TestRangedKey('a', 'g'),
                                                           new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertEquals(2, output.size());
    }

    @Test
    public void testGetsOnlyRangedValues() {
        final RangedLmdbStore<TestRangedKey, String> store = getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");
        store.put(new TestRangedKey('b', 'm'), "");
        store.put(new TestRangedKey('a', 'z'), "");

        final List<String> output = store.getValuesInRange(new TestRangedKey('a', 'a'),
                                                           new TestRangedKey('a', 'z'));

        store.close();
        Assert.assertEquals(2, output.size());
    }

    private RangedLmdbStore<TestRangedKey, String> getEmptyStore() {
        final File storeDir = Files.createTempDir();
        final Path path = storeDir.toPath();

        final RangedLmdbStore<TestRangedKey, String> store
            = new RangedLmdbStore<>(path, String.class);

        return store;
    }

}
