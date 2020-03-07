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
package com.bitvantage.bitvantagecaching.testhelpers;

import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.lmdb.KeyManager;
import com.bitvantage.bitvantagecaching.lmdb.Keys;
import com.bitvantage.bitvantagecaching.lmdb.RangedKeyManager;
import org.mockito.Mockito;
import com.bitvantage.bitvantagecaching.ValueSerializer;

/**
 *
 * @author Matt Laquidara
 */
public final class KeyValueHelpers {

    public static void mockNoValue(final ValueSerializer<String> serializer) 
            throws BitvantageStoreException {
        Mockito.when(serializer.getBytes(Mockito.any()))
                .thenReturn(new byte[1]);
        Mockito.when(serializer.getValue(Mockito.any())).thenReturn("");
    }

    public static void mockKeyOperations(
            final RangedKeyManager<TestPartitionKey, TestRangeKey> keyManager,
            final TestPartitionKey partition, final TestRangeKey range)
            throws BitvantageStoreException {
        final String key = makeKey(partition, range);
        Mockito.when(keyManager.createKeyString(partition, range))
                .thenReturn(key);
        Mockito.when(keyManager.materialize(key))
                .thenReturn(new Keys(partition, range));
    }

    public static void mockKeyOperations(
            final KeyManager<TestPartitionKey> keyManager,
            final TestPartitionKey partition) throws BitvantageStoreException {
        Mockito.when(keyManager.createKeyString(partition))
                .thenReturn(partition.getValue());
        Mockito.when(keyManager.materialize(partition.getValue()))
                .thenReturn(partition);
    }

    public static String makeKey(final TestPartitionKey partition,
                                 final TestRangeKey range) {
        return String.format("%s::%s", partition.getValue(), range.getValue());
    }
}
