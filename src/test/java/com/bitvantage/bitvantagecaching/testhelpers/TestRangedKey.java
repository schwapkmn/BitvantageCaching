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
package com.bitvantage.bitvantagecaching.testhelpers;

import com.bitvantage.bitvantagecaching.BitvantageStoreException;
import com.bitvantage.bitvantagecaching.KeyMaterializer;
import com.bitvantage.bitvantagecaching.RangedKey;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Value;

/**
 *
 * @author Matt Laquidara
 */
@Value
public class TestRangedKey extends RangedKey<TestRangedKey> {

    private final char unrangedPart;
    private final char rangedPart;

    @Override
    public TestRangedKey getRangeMin() {
        return new TestRangedKey(unrangedPart, 'a');
    }

    @Override
    public TestRangedKey getRangeMax() {
        return new TestRangedKey(unrangedPart, 'z');
    }

    @Override
    public String getKeyString() {
        return String.format("%c:%c", unrangedPart, rangedPart);
    }

    public static class Materializer implements KeyMaterializer<TestRangedKey> {

        final Pattern pattern = Pattern.compile("(.):(.)");

        @Override
        public TestRangedKey materialize(final String keyString) 
                throws BitvantageStoreException {
            final Matcher matcher = pattern.matcher(keyString);
            if (matcher.matches()) {
                final char unranged = matcher.group(1).charAt(0);
                final char ranged = matcher.group(2).charAt(0);
                return new TestRangedKey(unranged, ranged);
            }
            throw new BitvantageStoreException(String.format(
                    "Key string %s could not be materialized", keyString));
        }
    }
}
