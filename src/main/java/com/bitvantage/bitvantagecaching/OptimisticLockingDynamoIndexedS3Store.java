/*
 * Copyright 2020 Matt Laquidara.
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

import com.bitvantage.bitvantagecaching.dynamo.DynamoOptimisticLockingStore;
import com.bitvantage.bitvantagecaching.s3.S3Store;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author Matt Laquidara
 */
@RequiredArgsConstructor
public class OptimisticLockingDynamoIndexedS3Store<K extends PartitionKey, V>
        implements OptimisticLockingStore<K, V> {

    private final S3Store<UuidKey, V> s3;
    private final DynamoOptimisticLockingStore<K, UUID> dynamo;

    @Override
    public VersionedWrapper<V> get(final K key) throws BitvantageStoreException,
            InterruptedException {
        final VersionedWrapper<UUID> wrapper = dynamo.get(key);
        final UUID version = wrapper.getVersion();
        final UUID s3Key = wrapper.getValue();
        final V value = s3.get(new UuidKey(s3Key));
        return new VersionedWrapper(version, value);
    }

    @Override
    public Optional<V> putOnMatch(final K key, final V value, final UUID match)
            throws BitvantageStoreException, InterruptedException {
        final UUID s3Location = UUID.randomUUID();
        final UuidKey s3Key = new UuidKey(s3Location);
        s3.put(s3Key, value);

        final Optional<UUID> oldKey = dynamo.putOnMatch(
                key, s3Location, match);
        if (oldKey.isPresent()) {
            final UuidKey oldS3Key  = new UuidKey(oldKey.get());
            final V oldValue = s3.get(oldS3Key);
            s3.delete(oldS3Key);
            return Optional.of(oldValue);
        } else {
            s3.delete(s3Key);
        }
        return Optional.empty();
    }

    @Override
    public void put(final K key, final V value) throws BitvantageStoreException,
            InterruptedException {
        final UUID s3Location = UUID.randomUUID();
        final UuidKey s3Key = new UuidKey(s3Location);
        s3.put(s3Key, value);
        dynamo.put(key, s3Location);
    }

}
