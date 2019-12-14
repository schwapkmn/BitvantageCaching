/*
 * Copyright 2019 Matt Laquidara.
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

import java.util.Map;
import java.util.Set;
import lombok.Value;

/**
 *
 * @author Matt Laquidara
 */
@Value
public class CacheResult<P extends PartitionKey, V> {
    
    private final Map<P, V> cachedResults;
    private final Set<P> uncachedKeys; 
    
}