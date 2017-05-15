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

import com.bitvantage.bitvantagecaching.testhelpers.TestRangedKey;
import java.util.List;
import java.util.SortedMap;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Public Transit Analytics
 */
public class RangedNativeLmdbStoreTest {

    @Test
    public void testGetsNothing() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = NativeLmdbTestHelpers.getEmptyStore();

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'z'));

        Assert.assertTrue(output.isEmpty());
    }

    @Test
    public void testGetsValueInRange() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = NativeLmdbTestHelpers.getEmptyStore();
        store.put(new TestRangedKey('a', 'b'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'z'));

        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testGetsValueBeginningOfRange() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = NativeLmdbTestHelpers.getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'z'));

        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testGetsValueEndOfRange() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = NativeLmdbTestHelpers.getEmptyStore();
        store.put(new TestRangedKey('a', 'z'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'z'));

        Assert.assertEquals(1, output.size());
    }

    @Test
    public void testDoesNotGetValueBeyondEndOfRange() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = NativeLmdbTestHelpers.getEmptyStore();
        store.put(new TestRangedKey('a', 'z'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'y'));

        Assert.assertTrue(output.isEmpty());
    }

    @Test
    public void testDoesNotGetValueBeforeBeginningOfRange() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = NativeLmdbTestHelpers.getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'b'), new TestRangedKey('a', 'z'));

        Assert.assertTrue(output.isEmpty());
    }

    @Test
    public void testGetsAllValues() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = NativeLmdbTestHelpers.getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");
        store.put(new TestRangedKey('a', 'm'), "");
        store.put(new TestRangedKey('a', 'z'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'z'));

        Assert.assertEquals(3, output.size());
    }

    @Test
    public void testGetsOnlyValues() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = NativeLmdbTestHelpers.getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");
        store.put(new TestRangedKey('a', 'm'), "");
        store.put(new TestRangedKey('a', 'z'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'g'), new TestRangedKey('a', 'z'));

        Assert.assertEquals(2, output.size());
    }

    @Test
    public void testGetsOnlyRangedValues() throws Exception {
        final RangedNativeLmdbStore<TestRangedKey, String> store
                = NativeLmdbTestHelpers.getEmptyStore();
        store.put(new TestRangedKey('a', 'a'), "");
        store.put(new TestRangedKey('b', 'm'), "");
        store.put(new TestRangedKey('a', 'z'), "");

        final SortedMap<TestRangedKey, String> output = store.getValuesInRange(
                new TestRangedKey('a', 'a'), new TestRangedKey('a', 'z'));

        Assert.assertEquals(2, output.size());
    }

}
