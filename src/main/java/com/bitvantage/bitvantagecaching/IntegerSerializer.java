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

import com.google.common.primitives.Ints;
import com.bitvantage.bitvantagecaching.ValueSerializer;

/**
 *
 * @author Matt Laquidara
 */
public class IntegerSerializer implements ValueSerializer<Integer> {

    @Override
    public byte[] getBytes(final Integer value) {
        return Ints.toByteArray(value);
    }

    @Override
    public Integer getValue(byte[] bytes) {
        return Ints.fromByteArray(bytes);
    }
    
}
