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
package com.bitvantage.bitvantagecaching.dynamo;

import com.amazonaws.services.dynamodbv2.document.Expected;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.PartitionKey;
import com.bitvantage.bitvantagecaching.VersionedWrapper;
import java.nio.ByteBuffer;
import java.util.UUID;
import lombok.RequiredArgsConstructor;

/**
 *
 * @author Matt Laquidara
 */
@RequiredArgsConstructor
public class VersionedDynamoStoreSerializer<P extends PartitionKey, V> {

    private final DynamoStoreSerializer<P, V> serializer;
    
    public String getVersionKey() {
        return "version";
    }

    public Item serialize(final P partition, final V value) {
        final Item item = serializer.serialize(partition, value);
        final byte[] uuidBytes = getUuidBytes(UUID.randomUUID());
        return item.withBinary(getVersionKey(), uuidBytes);
    }

    public VersionedWrapper<V> deserializeValue(final Item item) 
            throws BitvantageStoreException {
        final V value = serializer.deserializeValue(item);
        final byte[] uuidBytes = item.getBinary(getVersionKey());
        final ByteBuffer buffer = ByteBuffer.wrap(uuidBytes);
        final long high = buffer.getLong();
        final long low = buffer.getLong();
        final UUID uuid = new UUID(high, low);
        return new VersionedWrapper(uuid, value);
    }

    public Expected getExpectation(final UUID match) {
        return new Expected(getVersionKey()).eq(getUuidBytes(match));
    }
    
    public String getPartitionKey(final P key) {
        return serializer.getPartitionKey(key);
    }

    public String getPartitionKeyName() {
        return serializer.getPartitionKeyName();
    }

    private static byte[] getUuidBytes(final UUID uuid) {
        final ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }

}
