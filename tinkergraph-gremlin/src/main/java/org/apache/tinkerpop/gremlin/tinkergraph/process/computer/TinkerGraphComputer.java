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
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TinkerGraphComputer implements GraphComputer {

    static {
        // GraphFilters are expensive w/ TinkerGraphComputer as everything is already in memory
        TraversalStrategies.GlobalCache.registerStrategies(TinkerGraphComputer.class,
                TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class).clone().removeStrategies(GraphFilterStrategy.class));
    }

    private ResultGraph resultGraph = null;
    private Persist persist = null;
    public volatile int round = 0;
    private VertexProgram<?> vertexProgram;
    private final TinkerGraph graph;
    private TinkerMemory memory;
    private final TinkerMessageBoard messageBoard = new TinkerMessageBoard();
    private boolean executed = false;
    private final Set<MapReduce> mapReducers = new HashSet<>();
    private int workers = Runtime.getRuntime().availableProcessors();
    private final GraphFilter graphFilter = new GraphFilter();

    private final ThreadFactory threadFactoryBoss = new BasicThreadFactory.Builder().namingPattern(TinkerGraphComputer.class.getSimpleName() + "-boss").build();

    /**
     * An {@code ExecutorService} that schedules up background work. Since a {@link GraphComputer} is only used once
     * for a {@link VertexProgram} a single threaded executor is sufficient.
     */
    private final ExecutorService computerService = Executors.newSingleThreadExecutor(threadFactoryBoss);

    public TinkerGraphComputer(final TinkerGraph graph) {
        this.graph = graph;
    }

    @Override
    public GraphComputer result(final ResultGraph resultGraph) {
        this.resultGraph = resultGraph;
        return this;
    }

    @Override
    public GraphComputer persist(final Persist persist) {
        this.persist = persist;
        return this;
    }

    @Override
    public GraphComputer program(final VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        return this;
    }

    @Override
    public GraphComputer mapReduce(final MapReduce mapReduce) {
        this.mapReducers.add(mapReduce);
        return this;
    }

    @Override
    public GraphComputer workers(final int workers) {
        this.workers = workers;
        return this;
    }

    @Override
    public GraphComputer vertices(final Traversal<Vertex, Vertex> vertexFilter) {
        this.graphFilter.setVertexFilter(vertexFilter);
        return this;
    }

    @Override
    public GraphComputer edges(final Traversal<Vertex, Edge> edgeFilter) {
        this.graphFilter.setEdgeFilter(edgeFilter);
        return this;
    }

    // 核心，主要用于执行vp和mr
    @Override
    public Future<ComputerResult> submit() {
        // 一个图计算任务只执行一次
        if (this.executed)
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        else
            this.executed = true;

        // 图计算任务必须要有vp或者mr才行
        if (null == this.vertexProgram && this.mapReducers.isEmpty())
            throw GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();

        // it is possible to run mapreducers without a vertex program
        // 如果没有vp，单独执行mr也是可以的
        if (null != this.vertexProgram) {
            GraphComputerHelper.validateProgramOnComputer(this, this.vertexProgram);
            this.mapReducers.addAll(this.vertexProgram.getMapReducers());
        }
        // get the result graph and persist state to use for the computation
        // 获取结果存储的类型，下面的方法内部使用了连续的三元运算符，可以学习
        this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
        this.persist = GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.persist));

        // 校验图写回类型和持久化类型的组合是否满足要求
        if (!this.features().supportsResultGraphPersistCombination(this.resultGraph, this.persist))
            throw GraphComputer.Exceptions.resultGraphPersistCombinationNotSupported(this.resultGraph, this.persist);

        // ensure requested workers are not larger than supported workers
        // 确保worker的数量不超过当前允许最多的worker数量，worker就理解为线程就行了
        if (this.workers > this.features().getMaxWorkers())
            throw GraphComputer.Exceptions.computerRequiresMoreWorkersThanSupported(this.workers, this.features().getMaxWorkers());

        // initialize the memory
        // 初始化Memory（TinkerMemory），主要是提取vp里面的MemoryComputeKey和mr里面的MapReduce
        this.memory = new TinkerMemory(this.vertexProgram, this.mapReducers);

        // 此处利用ExecutorService的submit方法，提供了一个异步执行线程
        // 里面的lambda是一个Callable，其实就是有返回类型的Runnable
        final Future<ComputerResult> result = computerService.submit(() -> { // Callable

            // 记录图计算开始的时间
            final long time = System.currentTimeMillis();

            // 创建GraphComputerView（GCV），一个图拥有GCV，代表着它进入了图计算模式
            // 创建GCV，主要做了以下的事情
            // 1. 利用graph和graphFilter参数，过滤G中的V和E
            // 2. 提取vp里面的VCK
            final TinkerGraphComputerView view = TinkerHelper.createGraphComputerView(this.graph, this.graphFilter, null != this.vertexProgram ? this.vertexProgram.getVertexComputeKeys() : Collections.emptySet());

            // workers，也就是线程池，用于在顶点上执行vp和mr，线程池主要做了以下几件事情
            // 1.分图，将所有顶点分堆，送给不同的worker执行
            // 2.创建worker内存
            // 3.初始化worker
            //final TinkerWorkerPool workers = new TinkerWorkerPool(this.graph, this.memory, this.workers);

            // 这里强行只使用单线程来执行图计算，适用于调试流程
            final TinkerWorkerPool workers = new TinkerWorkerPool(this.graph, this.memory, 1);

            try {

                // 执行vp
                if (null != this.vertexProgram) {
                    // execute the vertex program

                    // 在vp中初始化，接下来就需要看具体vp实现的内容了
                    this.vertexProgram.setup(this.memory);

                    // 死循环，要跳出，除非vp自己说算法执行完了，从内部break
                    while (true) {

                        if (Thread.interrupted()) throw new TraversalInterruptedException();

                        this.memory.completeSubRound();

                        // 告知workers，你要执行的是什么vp，把你的vp pool初始化好，准备干活
                        workers.setVertexProgram(this.vertexProgram);

                        // 核心中的核心，下面的lambda是一个三元组Consumer，在每一个顶点上执行vp
                        // 可以进入TinkerWorkerPool看执行方法，但是核心逻辑是在这个TriConsumer上
                        // 下面的三个参数在workers.executeVertexProgram内部提供，这些数据在前面初始化workers的时候已经设置好了
                        workers.executeVertexProgram((vertices, vertexProgram, workerMemory) -> { // TriConsumer
                            vertexProgram.workerIterationStart(workerMemory.asImmutable());
                            while (vertices.hasNext()) {
                                final Vertex vertex = vertices.next();
                                if (Thread.interrupted()) throw new TraversalInterruptedException();

                                // 最核心的地方
                                vertexProgram.execute(

                                        // 为该vertex生成一个ComputerVertex
                                        ComputerGraph.vertexProgram(vertex, vertexProgram),

                                        // 设置对应的消息发送器
                                        new TinkerMessenger<>(vertex, this.messageBoard, vertexProgram.getMessageCombiner()),

                                        // 对应的worker内存
                                        workerMemory);
                            }

                            // 结束一波vp执行，处理善后工作，进入下一位顶点选手的执行
                            vertexProgram.workerIterationEnd(workerMemory.asImmutable());
                            workerMemory.complete();
                            synchronized (TinkerGraphComputer.class) {
                                System.out.println("Current Thread=====" + Thread.currentThread().getName() + " " + ++workerMemory.round + " 轮执行完毕");
                            }

                        });

                        // 所有顶点执行这一轮完毕，进入下一轮整体执行
                        this.messageBoard.completeIteration();
                        this.memory.completeSubRound();

                        // 看看是否可以跳出苦海，结束苦逼的vp执行
                        if (this.vertexProgram.terminate(this.memory)) {
                            this.memory.incrIteration();
                            break;
                        } else {
                            this.memory.incrIteration();
                        }
                    }

                    // 抛弃所有的transient vck
                    view.complete(); // drop all transient vertex compute keys
                }

                // execute mapreduce jobs
                // 执行mr
                for (final MapReduce mapReduce : mapReducers) {
                    final TinkerMapEmitter<?, ?> mapEmitter = new TinkerMapEmitter<>(mapReduce.doStage(MapReduce.Stage.REDUCE));
                    final SynchronizedIterator<Vertex> vertices = new SynchronizedIterator<>(this.graph.vertices());
                    workers.setMapReduce(mapReduce);
                    workers.executeMapReduce(workerMapReduce -> {
                        workerMapReduce.workerStart(MapReduce.Stage.MAP);
                        while (true) {
                            if (Thread.interrupted()) throw new TraversalInterruptedException();
                            final Vertex vertex = vertices.next();
                            if (null == vertex) break;
                            workerMapReduce.map(ComputerGraph.mapReduce(vertex), mapEmitter);
                        }
                        workerMapReduce.workerEnd(MapReduce.Stage.MAP);
                    });
                    // sort results if a map output sort is defined
                    mapEmitter.complete(mapReduce);

                    // no need to run combiners as this is single machine
                    if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
                        final TinkerReduceEmitter<?, ?> reduceEmitter = new TinkerReduceEmitter<>();
                        final SynchronizedIterator<Map.Entry<?, Queue<?>>> keyValues = new SynchronizedIterator((Iterator) mapEmitter.reduceMap.entrySet().iterator());
                        workers.executeMapReduce(workerMapReduce -> {
                            workerMapReduce.workerStart(MapReduce.Stage.REDUCE);
                            while (true) {
                                if (Thread.interrupted()) throw new TraversalInterruptedException();
                                final Map.Entry<?, Queue<?>> entry = keyValues.next();
                                if (null == entry) break;
                                workerMapReduce.reduce(entry.getKey(), entry.getValue().iterator(), reduceEmitter);
                            }
                            workerMapReduce.workerEnd(MapReduce.Stage.REDUCE);
                        });
                        reduceEmitter.complete(mapReduce); // sort results if a reduce output sort is defined
                        mapReduce.addResultToMemory(this.memory, reduceEmitter.reduceQueue.iterator());
                    } else {
                        mapReduce.addResultToMemory(this.memory, mapEmitter.mapQueue.iterator());
                    }
                }
                // update runtime and return the newly computed graph
                this.memory.setRuntime(System.currentTimeMillis() - time);
                this.memory.complete(); // drop all transient properties and set iteration
                // determine the resultant graph based on the result graph/persist state
                final Graph resultGraph = view.processResultGraphPersist(this.resultGraph, this.persist);
                TinkerHelper.dropGraphComputerView(this.graph); // drop the view from the original source graph
                return new DefaultComputerResult(resultGraph, this.memory.asImmutable());
            } catch (InterruptedException ie) {
                workers.closeNow();
                throw new TraversalInterruptedException();
            } catch (Exception ex) {
                workers.closeNow();
                throw new RuntimeException(ex);
            } finally {
                workers.close();
            }
        });

        // 结束执行，返回结果
        this.computerService.shutdown();
        return result;
    }

    @Override
    public String toString() {
        return StringFactory.graphComputerString(this);
    }

    private static class SynchronizedIterator<V> {

        private final Iterator<V> iterator;

        public SynchronizedIterator(final Iterator<V> iterator) {
            this.iterator = iterator;
        }

        public synchronized V next() {
            return this.iterator.hasNext() ? this.iterator.next() : null;
        }
    }

    @Override
    public Features features() {
        return new Features() {

            @Override
            public int getMaxWorkers() {
                return Runtime.getRuntime().availableProcessors();
            }

            @Override
            public boolean supportsVertexAddition() {
                return false;
            }

            @Override
            public boolean supportsVertexRemoval() {
                return false;
            }

            @Override
            public boolean supportsVertexPropertyRemoval() {
                return false;
            }

            @Override
            public boolean supportsEdgeAddition() {
                return false;
            }

            @Override
            public boolean supportsEdgeRemoval() {
                return false;
            }

            @Override
            public boolean supportsEdgePropertyAddition() {
                return false;
            }

            @Override
            public boolean supportsEdgePropertyRemoval() {
                return false;
            }
        };
    }
}