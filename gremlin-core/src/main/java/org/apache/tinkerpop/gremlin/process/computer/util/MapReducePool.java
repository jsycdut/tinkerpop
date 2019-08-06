/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.computer.util;

import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * mr池
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MapReducePool {
    private final LinkedBlockingQueue<MapReduce<?, ?, ?, ?, ?>> pool;
    private static final int TIMEOUT_MS = 2500;

    public MapReducePool(final MapReduce mapReduce, final int poolSize) {
        this.pool = new LinkedBlockingQueue<>(poolSize);
        while (this.pool.remainingCapacity() > 0) {
            this.pool.add(mapReduce.clone());
        }
    }

    public MapReduce take() {
        try {
            return this.pool.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void offer(final MapReduce<?, ?, ?, ?, ?> mapReduce) {
        try {
            this.pool.offer(mapReduce, TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
