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
package org.apache.tinkerpop.gremlin.tinkergraph.process.computer;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MapReducePool;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramPool;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;
import org.apache.tinkerpop.gremlin.util.function.TriConsumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * 工作线程池，用于执行vp和mr
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TinkerWorkerPool implements AutoCloseable {

    // 工作线程以tinker-worker-开头
    // 调度此工作线程的调度线程以tinkergraphcomputer-boss开头
    private static final BasicThreadFactory THREAD_FACTORY_WORKER = new BasicThreadFactory.Builder().namingPattern("tinker-worker-%d").build();

    // 线程数量
    private final int numberOfWorkers;

    // 执行器
    private final ExecutorService workerPool;

    // 这个是负责管理一个异步提交的任务，和上面的workerPool一起使用
    private final CompletionService<Object> completionService;

    // vp池
    private VertexProgramPool vertexProgramPool;

    // mr池
    private MapReducePool mapReducePool;

    // worker使用的Memory，包含了主Memory和其他需要的MemoryComputeKey, aka MCK
    private final Queue<TinkerWorkerMemory> workerMemoryPool = new ConcurrentLinkedQueue<>();

    // 图里面的顶点，workerVertices的数量和workers数量一致
    // workerVertices里面的元素示意如下，1,2,3是属于第一个worker来处理，4,5,6由第二个worker来处理，其余类似
    // [ [1, 2, 3], [4, 5, 6], [7, 8, 9] ]
    private final List<List<Vertex>> workerVertices = new ArrayList<>();

    public TinkerWorkerPool(final TinkerGraph graph, final TinkerMemory memory, final int numberOfWorkers) {
        // 基础数据初始化
        this.numberOfWorkers = numberOfWorkers;
        this.workerPool = Executors.newFixedThreadPool(numberOfWorkers, THREAD_FACTORY_WORKER);
        this.completionService = new ExecutorCompletionService<>(this.workerPool);
        for (int i = 0; i < this.numberOfWorkers; i++) {
            this.workerMemoryPool.add(new TinkerWorkerMemory(memory));
            this.workerVertices.add(new ArrayList<>());
        }

        // 划分顶点，根据worker的数量，将顶点划分为n堆
        int batchSize = TinkerHelper.getVertices(graph).size() / this.numberOfWorkers;
        if (0 == batchSize)
            batchSize = 1;
        int counter = 0;
        int index = 0;

        // 将顶点装入worker对应的list的list内部
        List<Vertex> currentWorkerVertices = this.workerVertices.get(index);
        final Iterator<Vertex> iterator = graph.vertices();
        while (iterator.hasNext()) {
            final Vertex vertex = iterator.next();
            if (counter++ < batchSize || index == this.workerVertices.size() - 1) {
                currentWorkerVertices.add(vertex);
            } else {
                currentWorkerVertices = this.workerVertices.get(++index);
                currentWorkerVertices.add(vertex);
                counter = 1;
            }
        }
    }

    // 设定vp pool，用于为每一个worker设置一个vp池
    // 示意图如下
    // vp pool [ vp1        vp2      vp3        vp4 ]
    // workers [ worker1    worker2  worker3    worker4 ]
    public void setVertexProgram(final VertexProgram vertexProgram) {
        this.vertexProgramPool = new VertexProgramPool(vertexProgram, this.numberOfWorkers);
    }

    // 设定mr pool
    public void setMapReduce(final MapReduce mapReduce) {
        this.mapReducePool = new MapReducePool(mapReduce, this.numberOfWorkers);
    }

    // 执行vp，核心科技就在这里
    // 下面的TriConsumer是一个重要内容，为gremlin-core定义的三元消费者函数
    // 提供的这个worker将是及其重要的内容，处理完了这里，就可以返回去看GraphComputer的内容了
    public void executeVertexProgram(final TriConsumer<Iterator<Vertex>, VertexProgram, TinkerWorkerMemory> worker) throws InterruptedException {
        for (int i = 0; i < this.numberOfWorkers; i++) {
            final int index = i;

            this.completionService.submit(() -> { //callable
                final VertexProgram vp = this.vertexProgramPool.take();
                final TinkerWorkerMemory workerMemory = this.workerMemoryPool.poll();
                final List<Vertex> vertices = this.workerVertices.get(index);
                worker.accept(vertices.iterator(), vp, workerMemory);
                this.vertexProgramPool.offer(vp); // 插入vp
                this.workerMemoryPool.offer(workerMemory); // 插入workerMemory
                return null; // return null 是常见的把戏，因为把代码都交给了异步线程了，自己还返回个什么劲？
            });
        }

        // 直接take，如果没完成，会造成等待的情况
        for (int i = 0; i < this.numberOfWorkers; i++) {
            try {
                this.completionService.take().get();
            } catch (InterruptedException ie) {
                throw ie;
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    // 执行mr，和vp一样的套路，不一样的是mr的内容
    public void executeMapReduce(final Consumer<MapReduce> worker) throws InterruptedException {
        for (int i = 0; i < this.numberOfWorkers; i++) {
            this.completionService.submit(() -> {
                final MapReduce mr = this.mapReducePool.take();
                worker.accept(mr);
                this.mapReducePool.offer(mr);
                return null;
            });
        }
        for (int i = 0; i < this.numberOfWorkers; i++) {
            try {
                this.completionService.take().get();
            } catch (InterruptedException ie) {
                throw ie;
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    public void closeNow() throws Exception {
        this.workerPool.shutdownNow();
    }

    @Override
    public void close() throws Exception {
        this.workerPool.shutdown();
    }
}