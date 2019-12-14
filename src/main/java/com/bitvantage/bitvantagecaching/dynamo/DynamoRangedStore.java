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
package com.bitvantage.bitvantagecaching.dynamo;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.RangeKey;
import com.bitvantage.bitvantagecaching.RangedStore;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * @author Matt Laquidara
 */
public class DynamoRangedStore<P extends PartitionKey, R extends RangeKey<R>, V>
        implements RangedStore<P, R, V> {

    private static final int BATCH_SIZE = 25;

    private final String hashKeyName;
    private final String rangeKeyName;
    private final DynamoRangedStoreSerializer<P, R, V> serializer;

    private final Table table;
    private final DynamoDB dynamo;

    public DynamoRangedStore(final AmazonDynamoDB client,
                             final String table,
                             final DynamoRangedStoreSerializer<P, R, V> serializer) {
        this.dynamo = new DynamoDB(client);
        this.table = dynamo.getTable(table);
        this.hashKeyName = serializer.getPartitionKeyName();
        this.rangeKeyName = serializer.getRangeKeyName();
        this.serializer = serializer;
    }

    public Set<P> getPartitions() throws BitvantageStoreException {
        final ImmutableSet.Builder<P> builder = ImmutableSet.builder();
        final ItemCollection<ScanOutcome> items = table.scan();
        final Iterator<Item> iterator = items.iterator();
        while (iterator.hasNext()) {
            final Item result = iterator.next();
            final P key = serializer.deserializePartitionKey(result);
            builder.add(key);
        }
        return builder.build();
    }

    @Override
    public NavigableMap<R, V> getValuesInRange(
            final P partition, final R min, final R max)
            throws InterruptedException, BitvantageStoreException {
        final String hashValue = serializer.getPartitionKey(partition);
        final KeyAttribute hashKey = new KeyAttribute(hashKeyName, hashValue);

        final String minValue = serializer.getRangeKey(min);
        final String maxValue = serializer.getRangeKey(max);

        final RangeKeyCondition rangeKeyCondition
                = new RangeKeyCondition(rangeKeyName)
                        .between(minValue, maxValue);

        final QuerySpec querySpec = new QuerySpec().withHashKey(hashKey)
                .withRangeKeyCondition(rangeKeyCondition);

        return executeQuery(querySpec);
    }

    @Override
    public NavigableMap<R, V> getValuesAbove(final P partition, final R min)
            throws InterruptedException, BitvantageStoreException {
        final String hashValue = serializer.getPartitionKey(partition);
        final KeyAttribute hashKey = new KeyAttribute(hashKeyName, hashValue);

        final String minValue = serializer.getRangeKey(min);

        final RangeKeyCondition rangeKeyCondition
                = new RangeKeyCondition(rangeKeyName).ge(minValue);

        final QuerySpec querySpec = new QuerySpec().withHashKey(hashKey)
                .withRangeKeyCondition(rangeKeyCondition);

        return executeQuery(querySpec);
    }

    @Override
    public NavigableMap<R, V> getValuesBelow(final P partition, final R max)
            throws InterruptedException, BitvantageStoreException {
        final String hashValue = serializer.getPartitionKey(partition);
        final KeyAttribute hashKey = new KeyAttribute(hashKeyName, hashValue);

        final String maxValue = serializer.getRangeKey(max);

        final RangeKeyCondition rangeKeyCondition
                = new RangeKeyCondition(rangeKeyName).le(maxValue);

        final QuerySpec querySpec = new QuerySpec().withHashKey(hashKey)
                .withRangeKeyCondition(rangeKeyCondition);

        return executeQuery(querySpec);
    }

    @Override
    public NavigableMap<R, V> getNextValues(
            final P partition, final R min, final int count)
            throws InterruptedException, BitvantageStoreException {
        final String hashValue = serializer.getPartitionKey(partition);
        final KeyAttribute hashKey = new KeyAttribute(hashKeyName, hashValue);

        final String minValue = serializer.getRangeKey(min);

        final RangeKeyCondition rangeKeyCondition
                = new RangeKeyCondition(rangeKeyName).gt(minValue);

        final QuerySpec querySpec = new QuerySpec().withHashKey(hashKey)
                .withRangeKeyCondition(rangeKeyCondition)
                .withMaxResultSize(count);

        return executeQuery(querySpec);
    }

    @Override
    public NavigableMap<R, V> getHeadValues(final P partition, final int count)
            throws InterruptedException, BitvantageStoreException {
        final String hashValue = serializer.getPartitionKey(partition);
        final KeyAttribute hashKey = new KeyAttribute(hashKeyName, hashValue);

        final RangeKeyCondition rangeKeyCondition
                = new RangeKeyCondition(rangeKeyName);

        final QuerySpec querySpec = new QuerySpec().withHashKey(hashKey)
                .withRangeKeyCondition(rangeKeyCondition)
                .withMaxResultSize(count);

        return executeQuery(querySpec);
    }

    @Override
    public void put(final P partition, final R rangeValue,
                    final V value) throws BitvantageStoreException,
            InterruptedException {
        final Item item = serializer.serialize(partition, rangeValue, value);
        table.putItem(item);
    }

    @Override
    public void putAll(final P partition, final Map<R, V> entries)
            throws InterruptedException {
        final List<Item> items = entries.entrySet().stream()
                .map(entry -> serializer.serialize(partition, entry.getKey(),
                                                   entry.getValue()))
                .collect(Collectors.toList());
        final int total = items.size();

        int start = 0;

        while (start < total) {
            final int end = Math.min(total, start + BATCH_SIZE);
            final List<Item> subItems = items.subList(start, end);

            final TableWriteItems writeRequest = new TableWriteItems(
                    table.getTableName()).withItemsToPut(subItems);
            final BatchWriteItemOutcome outcome
                    = dynamo.batchWriteItem(writeRequest);
            Map<String, List<WriteRequest>> unprocessed
                    = outcome.getUnprocessedItems();
            while (unprocessed.size() > 0) {
                Thread.sleep(1000);
                final BatchWriteItemOutcome partialOutcome
                        = dynamo.batchWriteItemUnprocessed(unprocessed);
                unprocessed = partialOutcome.getUnprocessedItems();
            }
            start = end;
        }
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    private NavigableMap<R, V> executeQuery(final QuerySpec querySpec)
            throws BitvantageStoreException {
        final ItemCollection<QueryOutcome> result = table.query(querySpec);

        final ImmutableSortedMap.Builder<R, V> builder
                = ImmutableSortedMap.naturalOrder();
        for (final Item item : result) {
            final R rangeKey = serializer.deserializeRangeKey(item);
            final V distance = serializer.deserializeValue(item);

            builder.put(rangeKey, distance);
        }
        return builder.build();
    }

    @Override
    public NavigableMap<R, V> getPartition(final P partition) throws
            InterruptedException, BitvantageStoreException {
        final String hashValue = serializer.getPartitionKey(partition);
        final KeyAttribute hashKey = new KeyAttribute(hashKeyName, hashValue);
        final QuerySpec querySpec = new QuerySpec().withHashKey(hashKey);

        return executeQuery(querySpec);
    }

    @Override
    public boolean putIfAbsent(final P partition, final R range,
                               final V value)
            throws BitvantageStoreException, InterruptedException {
        final Item item = serializer.serialize(partition, range, value);
        try {
            table.putItem(item, new Expected(hashKeyName).notExist(),
                          new Expected(rangeKeyName).notExist());
        } catch (final ConditionalCheckFailedException e) {
            return false;
        }
        return true;
    }

}
