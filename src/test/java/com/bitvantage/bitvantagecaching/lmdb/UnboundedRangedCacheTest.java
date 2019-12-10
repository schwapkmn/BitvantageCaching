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
package com.bitvantage.bitvantagecaching.lmdb;

import com.bitvantage.bitvantagecaching.RangeStatus;
import com.bitvantage.bitvantagecaching.RangedStore;
import com.bitvantage.bitvantagecaching.UnboundedRangedCache;
import com.bitvantage.bitvantagecaching.testhelpers.TestPartitionKey;
import com.bitvantage.bitvantagecaching.testhelpers.TestRangeKey;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import java.util.NavigableMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 *
 * @author Matt Laquidara
 */
public class UnboundedRangedCacheTest {

    @Test
    public void testGetsPutRange() throws Exception {
        final RangedStore<TestPartitionKey, TestRangeKey, String> store
                = Mockito.mock(RangedStore.class);
        final UnboundedRangedCache<TestPartitionKey, TestRangeKey, String> cache
                = new UnboundedRangedCache<>(store);

        final TestPartitionKey partition = new TestPartitionKey("a");
        final TestRangeKey minKey = new TestRangeKey("a");
        final TestRangeKey maxKey = new TestRangeKey("z");
        final NavigableMap<TestRangeKey, String> values = ImmutableSortedMap.of(
                minKey, "a", maxKey, "z");

        Mockito.when(store.getValuesInRange(partition, minKey, maxKey))
                .thenReturn(values);

        cache.putRange(partition, minKey, maxKey, values);

        final RangeMap<TestRangeKey, RangeStatus<TestRangeKey, String>> output
                = cache.getRange(partition, minKey, maxKey);

        Assert.assertEquals(1, output.asMapOfRanges().size());
        Assert.assertTrue(output.asDescendingMapOfRanges().values().iterator()
                .next().isCached());
        Assert.assertEquals(values, output.asDescendingMapOfRanges().values()
                            .iterator().next().getValues());

    }

    @Test
    public void testGetsPutRangeWithInsertInRange() throws Exception {
        final RangedStore<TestPartitionKey, TestRangeKey, String> store
                = Mockito.mock(RangedStore.class);
        final UnboundedRangedCache<TestPartitionKey, TestRangeKey, String> cache
                = new UnboundedRangedCache<>(store);
        final TestPartitionKey partition = new TestPartitionKey("a");
        final TestRangeKey minKey = new TestRangeKey("a");
        final TestRangeKey midKey = new TestRangeKey("b");
        final TestRangeKey maxKey = new TestRangeKey("z");
        final NavigableMap<TestRangeKey, String> rangeValues
                = ImmutableSortedMap.of(minKey, "a", maxKey, "z");
        final NavigableMap<TestRangeKey, String> value
                = ImmutableSortedMap.<TestRangeKey, String>naturalOrder()
                        .putAll(rangeValues).put(midKey, "b").build();

        Mockito.when(store.getValuesInRange(partition, minKey, maxKey))
                .thenReturn(value);

        cache.putRange(partition, minKey, maxKey, rangeValues);
        cache.put(partition, midKey, "b");

        final RangeMap<TestRangeKey, RangeStatus<TestRangeKey, String>> output
                = cache.getRange(partition, minKey, maxKey);
        Assert.assertEquals(1, output.asMapOfRanges().size());
        Assert.assertTrue(output.asDescendingMapOfRanges().values().iterator()
                .next().isCached());
        Assert.assertEquals(value, output.asMapOfRanges().values().iterator()
                            .next().getValues());

    }

    @Test
    public void testGetsPutRangeWithInsertAroundKey() throws Exception {
        final RangedStore<TestPartitionKey, TestRangeKey, String> store
                = Mockito.mock(RangedStore.class);
        final UnboundedRangedCache<TestPartitionKey, TestRangeKey, String> cache
                = new UnboundedRangedCache<>(store);
        final TestPartitionKey partition = new TestPartitionKey("a");
        final TestRangeKey minKey = new TestRangeKey("a");
        final TestRangeKey midKey = new TestRangeKey("b");
        final TestRangeKey maxKey = new TestRangeKey("z");
        final NavigableMap<TestRangeKey, String> rangeValues
                = ImmutableSortedMap.of(minKey, "a", maxKey, "z");
        final NavigableMap<TestRangeKey, String> value
                = ImmutableSortedMap.<TestRangeKey, String>naturalOrder()
                        .putAll(rangeValues).put(midKey, "b").build();

        Mockito.when(store.getValuesInRange(partition, minKey, maxKey))
                .thenReturn(value);

        cache.put(partition, midKey, "b");
        cache.putRange(partition, minKey, maxKey,
                       ImmutableSortedMap.of(minKey, "a", maxKey, "z"));

        final RangeMap<TestRangeKey, RangeStatus<TestRangeKey, String>> output
                = cache.getRange(partition, minKey, maxKey);
        Assert.assertEquals(1, output.asMapOfRanges().size());
        Assert.assertTrue(output.asDescendingMapOfRanges().values().iterator()
                .next().isCached());
        Assert.assertEquals(value, output.asMapOfRanges().values().iterator()
                            .next().getValues());
    }

    @Test
    public void testHasDiscontinuity() throws Exception {
        final RangedStore<TestPartitionKey, TestRangeKey, String> store
                = Mockito.mock(RangedStore.class);
        final UnboundedRangedCache<TestPartitionKey, TestRangeKey, String> cache
                = new UnboundedRangedCache<>(store);
        final TestPartitionKey partition = new TestPartitionKey("a");
        final TestRangeKey range1MinKey = new TestRangeKey("a");
        final TestRangeKey range1MaxKey = new TestRangeKey("b");
        final TestRangeKey range2MinKey = new TestRangeKey("y");
        final TestRangeKey range2MaxKey = new TestRangeKey("z");

        final NavigableMap<TestRangeKey, String> range1Values
                = ImmutableSortedMap.of(range1MinKey, "a",
                                        range1MaxKey, "z");

        final NavigableMap<TestRangeKey, String> range2Values
                = ImmutableSortedMap.of(range2MinKey, "a",
                                        range2MaxKey, "z");
//        final NavigableMap<TestRangeKey, String> value = ImmutableSortedMap
//                .<TestRangeKey, String>naturalOrder().putAll(range1Values)
//                .putAll(range2Values).build();

        Mockito.when(store.getValuesInRange(partition, range1MinKey,
                                            range1MaxKey))
                .thenReturn(range1Values);
        Mockito.when(store.getValuesInRange(partition, range2MinKey,
                                            range2MaxKey))
                .thenReturn(range2Values);

        cache.putRange(partition, range1MinKey, range1MaxKey,
                       range1Values);

        cache.putRange(partition, range2MinKey, range2MaxKey,
                       range2Values);

        final RangeMap<TestRangeKey, RangeStatus<TestRangeKey, String>> output
                = cache.getRange(partition, range1MinKey, range2MaxKey);

        Assert.assertEquals(3, output.asMapOfRanges().size());
        Assert.assertTrue(output.asMapOfRanges().get(Range
                .closed(range1MinKey, range1MaxKey)).isCached());
        Assert.assertTrue(output.asMapOfRanges().get(Range.closed(
                range2MinKey, range2MaxKey)).isCached());
        Assert.assertFalse(output.asMapOfRanges().get(Range.open(
                range1MaxKey, range2MinKey)).isCached());

    }

}
