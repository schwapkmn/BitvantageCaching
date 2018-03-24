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

import com.bitvantage.bitvantagecaching.mocks.MapRangedStore;
import com.bitvantage.bitvantagecaching.testhelpers.TestRangedKey;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import java.util.TreeMap;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Matt Laquidara
 */
public class UnboundedRangedCacheTest {

    @Test
    public void testGetsPutRange() throws Exception {
        final MapRangedStore<TestRangedKey, String> store
                = new MapRangedStore<TestRangedKey, String>(new TreeMap<>());
        final UnboundedRangedCache<TestRangedKey, String> cache
                = new UnboundedRangedCache<>(store);
        final TestRangedKey minKey = new TestRangedKey('a', 'a');
        final TestRangedKey maxKey = new TestRangedKey('z', 'z');
        cache.putRange(minKey, maxKey, ImmutableSortedMap.of(minKey, "a",
                                                             maxKey, "z"));
        final RangeMap<TestRangedKey, RangeStatus<TestRangedKey, String>> output
                = cache.getRange(minKey, maxKey);
        Assert.assertEquals(1, output.asMapOfRanges().size());
        Assert.assertTrue(output.asDescendingMapOfRanges().values().iterator()
                .next().isCached());
        Assert.assertEquals(ImmutableSortedMap.of(minKey, "a", maxKey, "z"),
                            output.asDescendingMapOfRanges().values().iterator()
                                    .next().getValues());

    }

    @Test
    public void testGetsPutRangeWithInsertInRange() throws Exception {
        final MapRangedStore<TestRangedKey, String> store
                = new MapRangedStore<TestRangedKey, String>(new TreeMap<>());
        final UnboundedRangedCache<TestRangedKey, String> cache
                = new UnboundedRangedCache<>(store);
        final TestRangedKey minKey = new TestRangedKey('a', 'a');
        final TestRangedKey midKey = new TestRangedKey('a', 'b');
        final TestRangedKey maxKey = new TestRangedKey('z', 'z');
        cache.putRange(minKey, maxKey, ImmutableSortedMap.of(minKey, "a",
                                                             maxKey, "z"));
        cache.put(midKey, "b");
        final RangeMap<TestRangedKey, RangeStatus<TestRangedKey, String>> output
                = cache.getRange(minKey, maxKey);
        Assert.assertEquals(1, output.asMapOfRanges().size());
        Assert.assertTrue(output.asDescendingMapOfRanges().values().iterator()
                .next().isCached());
        Assert.assertEquals(ImmutableSortedMap.of(minKey, "a", midKey, "b",
                                                  maxKey, "z"),
                            output.asMapOfRanges().values().iterator()
                                    .next().getValues());

    }

    @Test
    public void testGetsPutRangeWithInsertAroundKey() throws Exception {
        final MapRangedStore<TestRangedKey, String> store
                = new MapRangedStore<TestRangedKey, String>(new TreeMap<>());
        final UnboundedRangedCache<TestRangedKey, String> cache
                = new UnboundedRangedCache<>(store);
        final TestRangedKey minKey = new TestRangedKey('a', 'a');
        final TestRangedKey midKey = new TestRangedKey('a', 'b');
        final TestRangedKey maxKey = new TestRangedKey('z', 'z');
        cache.put(midKey, "b");
        cache.putRange(minKey, maxKey, ImmutableSortedMap.of(minKey, "a",
                                                             maxKey, "z"));
        final RangeMap<TestRangedKey, RangeStatus<TestRangedKey, String>> output
                = cache.getRange(minKey, maxKey);
        Assert.assertEquals(1, output.asMapOfRanges().size());
        Assert.assertTrue(output.asDescendingMapOfRanges().values().iterator()
                .next().isCached());
        Assert.assertEquals(ImmutableSortedMap.of(minKey, "a", midKey, "b",
                                                  maxKey, "z"),
                            output.asMapOfRanges().values().iterator()
                                    .next().getValues());
    }

    @Test
    public void testHasDiscontinuity() throws Exception {
        final MapRangedStore<TestRangedKey, String> store
                = new MapRangedStore<TestRangedKey, String>(new TreeMap<>());
        final UnboundedRangedCache<TestRangedKey, String> cache
                = new UnboundedRangedCache<>(store);
        final TestRangedKey range1MinKey = new TestRangedKey('a', 'a');
        final TestRangedKey range1MaxKey = new TestRangedKey('a', 'b');
        final TestRangedKey range2MinKey = new TestRangedKey('z', 'y');
        final TestRangedKey range2MaxKey = new TestRangedKey('z', 'z');
        cache.putRange(range1MinKey, range1MaxKey, ImmutableSortedMap.of(
                       range1MinKey, "a",
                       range1MaxKey, "z"));
        cache.putRange(range2MinKey, range2MaxKey, ImmutableSortedMap.of(
                       range2MinKey, "a",
                       range2MaxKey, "z"));
        final RangeMap<TestRangedKey, RangeStatus<TestRangedKey, String>> output
                = cache.getRange(range1MinKey, range2MaxKey);
        Assert.assertEquals(3, output.asMapOfRanges().size());
        Assert.assertTrue(output.asMapOfRanges().get(Range
                .closed(range1MinKey, range1MaxKey)).isCached());
        Assert.assertTrue(output.asMapOfRanges().get(Range.closed(
                range2MinKey, range2MaxKey)).isCached());
        Assert.assertFalse(output.asMapOfRanges().get(Range.open(
                range1MaxKey, range2MinKey)).isCached());
       
    }

}
