/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bitvantage.bitvantagecaching;

import lombok.RequiredArgsConstructor;

/**
 *
 * @author Public Transit Analytics
 */
@RequiredArgsConstructor
public class LongKey implements RangeKey<LongKey> {

    private static final LongKey MIN
            = new LongKey(0);
    private static final LongKey MAX
            = new LongKey(0xFFFFFFFFFFFFFFFFL);

    private final long hash;

    @Override
    public LongKey getRangeMin() {
        return MIN;
    }

    @Override
    public LongKey getRangeMax() {
        return MAX;
    }

    @Override
    public int compareTo(final LongKey o) {
        return Long.compare(hash, o.hash);
    }

}
