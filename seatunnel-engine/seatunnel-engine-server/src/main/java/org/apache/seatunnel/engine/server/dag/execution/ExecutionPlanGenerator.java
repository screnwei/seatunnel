/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.dag.execution;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleAction;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleConfig;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleMultipleRowStrategy;
import org.apache.seatunnel.engine.core.dag.actions.ShuffleStrategy;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkConfig;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.core.dag.actions.UnknownActionException;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class ExecutionPlanGenerator {
    private final LogicalDag logicalPlan;
    private final JobImmutableInformation jobImmutableInformation;
    private final EngineConfig engineConfig;
    private final IdGenerator idGenerator = new IdGenerator();

    public ExecutionPlanGenerator(
            @NonNull LogicalDag logicalPlan,
            @NonNull JobImmutableInformation jobImmutableInformation,
            @NonNull EngineConfig engineConfig) {
        checkArgument(
                logicalPlan.getEdges().size() > 0, "ExecutionPlan Builder must have LogicalPlan.");
        this.logicalPlan = logicalPlan;
        this.jobImmutableInformation = jobImmutableInformation;
        this.engineConfig = engineConfig;
    }

    public ExecutionPlan generate() {
        log.debug("Generate execution plan using logical plan:");

        Set<ExecutionEdge> executionEdges = generateExecutionEdges(logicalPlan.getEdges());
        log.debug("Phase 1: generate execution edge list {}", executionEdges);

        executionEdges = generateShuffleEdges(executionEdges);
        log.debug("Phase 2: generate shuffle edge list {}", executionEdges);

        executionEdges = generateTransformChainEdges(executionEdges);
        log.debug("Phase 3: generate transform chain edge list {}", executionEdges);

        List<Pipeline> pipelines = generatePipelines(executionEdges);
        log.debug("Phase 4: generate pipeline list {}", pipelines);

        ExecutionPlan executionPlan = new ExecutionPlan(pipelines, jobImmutableInformation);
        log.debug("Phase 5: generate execution plan: {}", executionPlan);

        return executionPlan;
    }

    public static Action recreateAction(Action action, Long id, int parallelism) {
        Action newAction;
        if (action instanceof ShuffleAction) {
            newAction =
                    new ShuffleAction(id, action.getName(), ((ShuffleAction) action).getConfig());
        } else if (action instanceof SinkAction) {
            newAction =
                    new SinkAction<>(
                            id,
                            action.getName(),
                            new ArrayList<>(),
                            ((SinkAction<?, ?, ?, ?>) action).getSink(),
                            action.getJarUrls(),
                            action.getConnectorJarIdentifiers(),
                            (SinkConfig) action.getConfig());
        } else if (action instanceof SourceAction) {
            newAction =
                    new SourceAction<>(
                            id,
                            action.getName(),
                            ((SourceAction<?, ?, ?>) action).getSource(),
                            action.getJarUrls(),
                            action.getConnectorJarIdentifiers());
        } else if (action instanceof TransformAction) {
            newAction =
                    new TransformAction(
                            id,
                            action.getName(),
                            ((TransformAction) action).getTransform(),
                            action.getJarUrls(),
                            action.getConnectorJarIdentifiers());
        } else if (action instanceof TransformChainAction) {
            newAction =
                    new TransformChainAction(
                            id,
                            action.getName(),
                            action.getJarUrls(),
                            action.getConnectorJarIdentifiers(),
                            ((TransformChainAction<?>) action).getTransforms());
        } else {
            throw new UnknownActionException(action);
        }
        newAction.setParallelism(parallelism);
        return newAction;
    }

    /**
     * /入参是逻辑计划的边，每个边存储了上下游的节点
     * @param logicalEdges
     * @return
     */
    private Set<ExecutionEdge> generateExecutionEdges(Set<LogicalEdge> logicalEdges) {
        Set<ExecutionEdge> executionEdges = new LinkedHashSet<>();

        Map<Long, ExecutionVertex> logicalVertexIdToExecutionVertexMap = new HashMap();
        // 按照顺序进行排序，首先按照输入节点的顺序进行排序，当输入节点相同时，按照输出节点进行排序
        List<LogicalEdge> sortedLogicalEdges = new ArrayList<>(logicalEdges);
        Collections.sort(
                sortedLogicalEdges,
                (o1, o2) -> {
                    if (!o1.getInputVertexId().equals(o2.getInputVertexId())) {
                        return o1.getInputVertexId() > o2.getInputVertexId() ? 1 : -1;
                    }
                    if (!o1.getTargetVertexId().equals(o2.getTargetVertexId())) {
                        return o1.getTargetVertexId() > o2.getTargetVertexId() ? 1 : -1;
                    }
                    return 0;
                });
        // 循环将每个逻辑计划的边转换为执行计划的边
        for (LogicalEdge logicalEdge : sortedLogicalEdges) {
            LogicalVertex logicalInputVertex = logicalEdge.getInputVertex();
            ExecutionVertex executionInputVertex =
                    logicalVertexIdToExecutionVertexMap.computeIfAbsent(
                            logicalInputVertex.getVertexId(),
                            vertexId -> {
                                long newId = idGenerator.getNextId();
                                Action newLogicalInputAction =
                                        recreateAction(
                                                logicalInputVertex.getAction(),
                                                newId,
                                                logicalInputVertex.getParallelism());
                                return new ExecutionVertex(
                                        newId,
                                        newLogicalInputAction,
                                        logicalInputVertex.getParallelism());
                            });

            // 与输入节点类似，重新创建执行计划节点
            LogicalVertex logicalTargetVertex = logicalEdge.getTargetVertex();
            ExecutionVertex executionTargetVertex =
                    logicalVertexIdToExecutionVertexMap.computeIfAbsent(
                            logicalTargetVertex.getVertexId(),
                            vertexId -> {
                                long newId = idGenerator.getNextId();
                                Action newLogicalTargetAction =
                                        recreateAction(
                                                logicalTargetVertex.getAction(),
                                                newId,
                                                logicalTargetVertex.getParallelism());
                                return new ExecutionVertex(
                                        newId,
                                        newLogicalTargetAction,
                                        logicalTargetVertex.getParallelism());
                            });

            // 生成执行计划的边
            ExecutionEdge executionEdge =
                    new ExecutionEdge(executionInputVertex, executionTargetVertex);
            executionEdges.add(executionEdge);
        }
        return executionEdges;
    }

    @SuppressWarnings("MagicNumber")
    private Set<ExecutionEdge> generateShuffleEdges(Set<ExecutionEdge> executionEdges) {
        // 以上游节点编号为key，list存储下游所有节点
        Map<Long, List<ExecutionVertex>> targetVerticesMap = new LinkedHashMap<>();
        // 仅存储类型为Source的节点
        Set<ExecutionVertex> sourceExecutionVertices = new HashSet<>();
        executionEdges.forEach(
                edge -> {
                    ExecutionVertex leftVertex = edge.getLeftVertex();
                    ExecutionVertex rightVertex = edge.getRightVertex();
                    if (leftVertex.getAction() instanceof SourceAction) {
                        sourceExecutionVertices.add(leftVertex);
                    }
                    targetVerticesMap
                            .computeIfAbsent(leftVertex.getVertexId(), id -> new ArrayList<>())
                            .add(rightVertex);
                });
        if (sourceExecutionVertices.size() != 1) {
            return executionEdges;
        }
        ExecutionVertex sourceExecutionVertex = sourceExecutionVertices.stream().findFirst().get();
        Action sourceAction = sourceExecutionVertex.getAction();
        List<CatalogTable> producedCatalogTables = new ArrayList<>();
        if (sourceAction instanceof SourceAction) {
            try {
                producedCatalogTables =
                        ((SourceAction<?, ?, ?>) sourceAction)
                                .getSource()
                                .getProducedCatalogTables();
            } catch (UnsupportedOperationException e) {
            }
        } else if (sourceAction instanceof TransformChainAction) {
            return executionEdges;
        } else {
            throw new SeaTunnelException(
                    "source action must be SourceAction or TransformChainAction");
        }
        // 数据源仅产生单表或
        // 数据源仅有一个下游输出时，直接返回
        if (producedCatalogTables.size() <= 1
                || targetVerticesMap.get(sourceExecutionVertex.getVertexId()).size() <= 1) {
            return executionEdges;
        }

        List<ExecutionVertex> sinkVertices =
                targetVerticesMap.get(sourceExecutionVertex.getVertexId());
        // 检查是否有其他类型的Action，在当前步骤下游节点尽可能有两种类型，Transform与Sink，这里是判断仅能有Sink类型
        Optional<ExecutionVertex> hasOtherAction =
                sinkVertices.stream()
                        .filter(vertex -> !(vertex.getAction() instanceof SinkAction))
                        .findFirst();
        checkArgument(!hasOtherAction.isPresent());
        // 当以上代码全部走完之后，当前的场景为：
        // 仅有一个数据源，该数据源会产生多张表，下游还有多个sink节点依赖与产生的多表
        // 也就是说当前任务仅有两类节点，一个会产生多张表的Source节点，一组依赖与该Source的Sink节点
        // 那么会新生成一个shuffle节点，添加到两者之间
        // 将依赖关系修改与source->shuffle->多个sink

        Set<ExecutionEdge> newExecutionEdges = new LinkedHashSet<>();
        ShuffleStrategy shuffleStrategy =
                ShuffleMultipleRowStrategy.builder()
                        .jobId(jobImmutableInformation.getJobId())
                        .inputPartitions(sourceAction.getParallelism())
                        .catalogTables(producedCatalogTables)
                        .queueEmptyQueueTtl(
                                (int)
                                        (engineConfig.getCheckpointConfig().getCheckpointInterval()
                                                * 3))
                        .build();
        ShuffleConfig shuffleConfig =
                ShuffleConfig.builder().shuffleStrategy(shuffleStrategy).build();

        long shuffleVertexId = idGenerator.getNextId();
        String shuffleActionName = String.format("Shuffle [%s]", sourceAction.getName());
        ShuffleAction shuffleAction =
                new ShuffleAction(shuffleVertexId, shuffleActionName, shuffleConfig);
        shuffleAction.setParallelism(sourceAction.getParallelism());
        ExecutionVertex shuffleVertex =
                new ExecutionVertex(shuffleVertexId, shuffleAction, shuffleAction.getParallelism());
        ExecutionEdge sourceToShuffleEdge = new ExecutionEdge(sourceExecutionVertex, shuffleVertex);
        newExecutionEdges.add(sourceToShuffleEdge);

        // 将多个sink节点的并行度修改为1
        for (ExecutionVertex sinkVertex : sinkVertices) {
            sinkVertex.setParallelism(1);
            sinkVertex.getAction().setParallelism(1);
            ExecutionEdge shuffleToSinkEdge = new ExecutionEdge(shuffleVertex, sinkVertex);
            newExecutionEdges.add(shuffleToSinkEdge);
        }
        return newExecutionEdges;
    }

    private Set<ExecutionEdge> generateTransformChainEdges(Set<ExecutionEdge> executionEdges) {
        // 使用了三个结构，存储所有的Source节点，以及每个输入，输出节点
        // inputVerticesMap中以下游节点id为key，存储了所有的上游输入节点
        // targetVerticesMap则以上游节点id为key，存储了所有的下游输出节点
        Map<Long, List<ExecutionVertex>> inputVerticesMap = new HashMap<>();
        Map<Long, List<ExecutionVertex>> targetVerticesMap = new HashMap<>();
        Set<ExecutionVertex> sourceExecutionVertices = new HashSet<>();
        executionEdges.forEach(
                edge -> {
                    ExecutionVertex leftVertex = edge.getLeftVertex();
                    ExecutionVertex rightVertex = edge.getRightVertex();
                    if (leftVertex.getAction() instanceof SourceAction) {
                        sourceExecutionVertices.add(leftVertex);
                    }
                    inputVerticesMap
                            .computeIfAbsent(rightVertex.getVertexId(), id -> new ArrayList<>())
                            .add(leftVertex);
                    targetVerticesMap
                            .computeIfAbsent(leftVertex.getVertexId(), id -> new ArrayList<>())
                            .add(rightVertex);
                });

        Map<Long, ExecutionVertex> transformChainVertexMap = new HashMap<>();
        Map<Long, Long> chainedTransformVerticesMapping = new HashMap<>();
        // 对每个source进行循环，即从DAG中所有的头节点开始变量
        for (ExecutionVertex sourceVertex : sourceExecutionVertices) {
            List<ExecutionVertex> vertices = new ArrayList<>();
            vertices.add(sourceVertex);
            for (int index = 0; index < vertices.size(); index++) {
                ExecutionVertex vertex = vertices.get(index);

                fillChainedTransformExecutionVertex(
                        vertex,
                        chainedTransformVerticesMapping,
                        transformChainVertexMap,
                        executionEdges,
                        Collections.unmodifiableMap(inputVerticesMap),
                        Collections.unmodifiableMap(targetVerticesMap));
                // 当当前节点存在下游节点时，将所有下游节点放入list中，二层循环会重新计算刚刚加入进去的下游节点，可能是Transform节点也可能是Sink节点
                if (targetVerticesMap.containsKey(vertex.getVertexId())) {
                    vertices.addAll(targetVerticesMap.get(vertex.getVertexId()));
                }
            }
        }

        // 循环完成，会将可以链化的Transform节点进行链化，在链化过程中会将可以链化的关系边从执行计划中删除
        // 所以此时的逻辑计划已经无法构成图的关系，需要重新构建
        Set<ExecutionEdge> transformChainEdges = new LinkedHashSet<>();
        // 对现存关系进行循环
        for (ExecutionEdge executionEdge : executionEdges) {
            ExecutionVertex leftVertex = executionEdge.getLeftVertex();
            ExecutionVertex rightVertex = executionEdge.getRightVertex();
            boolean needRebuild = false;
            // 会从链化的map中查询当前边的输入，输出节点
            // 如果在链化的map中存在，则表明该节点已经被链化，需要从映射关系中找到链化之后的节点
            // 重新修正DAG
            if (chainedTransformVerticesMapping.containsKey(leftVertex.getVertexId())) {
                needRebuild = true;
                leftVertex =
                        transformChainVertexMap.get(
                                chainedTransformVerticesMapping.get(leftVertex.getVertexId()));
            }
            if (chainedTransformVerticesMapping.containsKey(rightVertex.getVertexId())) {
                needRebuild = true;
                rightVertex =
                        transformChainVertexMap.get(
                                chainedTransformVerticesMapping.get(rightVertex.getVertexId()));
            }
            if (needRebuild) {
                executionEdge = new ExecutionEdge(leftVertex, rightVertex);
            }
            transformChainEdges.add(executionEdge);
        }
        return transformChainEdges;
    }

    private void fillChainedTransformExecutionVertex(
            ExecutionVertex currentVertex,
            Map<Long, Long> chainedTransformVerticesMapping,
            Map<Long, ExecutionVertex> transformChainVertexMap,
            Set<ExecutionEdge> executionEdges,
            Map<Long, List<ExecutionVertex>> inputVerticesMap,
            Map<Long, List<ExecutionVertex>> targetVerticesMap) {

        // 当map中以及包含当前节点则退出
        if (chainedTransformVerticesMapping.containsKey(currentVertex.getVertexId())) {
            return;
        }

        List<ExecutionVertex> transformChainedVertices = new ArrayList<>();
        collectChainedVertices(
                currentVertex,
                transformChainedVertices,
                executionEdges,
                inputVerticesMap,
                targetVerticesMap);

        // 当list不为空时，表示list里面的transform节点可以被合并成一个
        if (transformChainedVertices.size() > 0) {
            long newVertexId = idGenerator.getNextId();
            List<SeaTunnelTransform> transforms = new ArrayList<>(transformChainedVertices.size());
            List<String> names = new ArrayList<>(transformChainedVertices.size());
            Set<URL> jars = new HashSet<>();
            Set<ConnectorJarIdentifier> identifiers = new HashSet<>();

            transformChainedVertices.stream()
                    .peek(
                        // 在mapping中添加所有历史节点编号与新节点编号的映射
                        vertex ->
                                    chainedTransformVerticesMapping.put(
                                            vertex.getVertexId(), newVertexId))
                    .map(ExecutionVertex::getAction)
                    .map(action -> (TransformAction) action)
                    .forEach(
                            action -> {
                                transforms.add(action.getTransform());
                                jars.addAll(action.getJarUrls());
                                identifiers.addAll(action.getConnectorJarIdentifiers());
                                names.add(action.getName());
                            });
            String transformChainActionName =
                    String.format("TransformChain[%s]", String.join("->", names));
            // 将多个TransformAction合并成一个TransformChainAction
            TransformChainAction transformChainAction =
                    new TransformChainAction(
                            newVertexId, transformChainActionName, jars, identifiers, transforms);
            transformChainAction.setParallelism(currentVertex.getAction().getParallelism());

            ExecutionVertex executionVertex =
                    new ExecutionVertex(
                            newVertexId, transformChainAction, currentVertex.getParallelism());
            // 在状态中将修改完成的节点信息放入
            transformChainVertexMap.put(newVertexId, executionVertex);
            chainedTransformVerticesMapping.put(
                    currentVertex.getVertexId(), executionVertex.getVertexId());
        }
    }

    private void collectChainedVertices(
            ExecutionVertex currentVertex,
            List<ExecutionVertex> chainedVertices,
            Set<ExecutionEdge> executionEdges,
            Map<Long, List<ExecutionVertex>> inputVerticesMap,
            Map<Long, List<ExecutionVertex>> targetVerticesMap) {
        Action action = currentVertex.getAction();
        // Currently only support Transform action chaining.
        // 仅对TransformAction进行合并
        if (action instanceof TransformAction) {
            if (chainedVertices.size() == 0) {
                // 需要进行合并的节点list为空时，将自身添加到list中
                // 进入该分支的条件为当前节点为TransformAction并且所需链化列表为空
                // 此时可能有几种场景：第一个Transform节点进入，该Transform节点无任何限制
                chainedVertices.add(currentVertex);
            } else if (inputVerticesMap.get(currentVertex.getVertexId()).size() == 1) {
                // It cannot be chained to any input vertex if it has multiple input vertices.
                // 当进入该条件分支则表明：
                // 所需链化的列表chainedVertices已经至少有一个TransformAction了
                // 此时的场景为：上游的Transform节点仅有一个下游节点，即当前节点。此限制是由下方的判断保证
                // 将当前TransformAction节点与上一个TransformAction节点进行链化
                // 在执行计划中将该关系删除
                executionEdges.remove(
                        new ExecutionEdge(
                                chainedVertices.get(chainedVertices.size() - 1), currentVertex));
                // 将自身加入需要链化的list中
                chainedVertices.add(currentVertex);
            } else {
                return;
            }
        } else {
            return;
        }

        // It cannot chain to any target vertex if it has multiple target vertices.
        if (targetVerticesMap.get(currentVertex.getVertexId()).size() == 1) {
            // 当当前节点仅有一个下游节点时，再次尝试链化
            // 如果当前节点存在多个下游节点，则不会将下游的节点进行链化，所以能保证上面的链化时两个节点是一对一的关系
            // 这里会调用的场景为Transform节点仅有一个下游节点
            collectChainedVertices(
                    targetVerticesMap.get(currentVertex.getVertexId()).get(0),
                    chainedVertices,
                    executionEdges,
                    inputVerticesMap,
                    targetVerticesMap);
        }
    }

    private List<Pipeline> generatePipelines(Set<ExecutionEdge> executionEdges) {
        // 存储每个执行计划节点
        Set<ExecutionVertex> executionVertices = new LinkedHashSet<>();
        for (ExecutionEdge edge : executionEdges) {
            executionVertices.add(edge.getLeftVertex());
            executionVertices.add(edge.getRightVertex());
        }
        // 调用Pipeline执行器将执行计划转换为Pipeline
        PipelineGenerator pipelineGenerator =
                new PipelineGenerator(executionVertices, new ArrayList<>(executionEdges));
        List<Pipeline> pipelines = pipelineGenerator.generatePipelines();

        Set<String> duplicatedActionNames = new HashSet<>();
        Set<String> actionNames = new HashSet<>();
        for (Pipeline pipeline : pipelines) {
            Integer pipelineId = pipeline.getId();
            for (ExecutionVertex vertex : pipeline.getVertexes().values()) {
                // 获取当前Pipeline的每个执行节点，重新设置Action的名称，添加了pipeline的名称
                Action action = vertex.getAction();
                String actionName = String.format("pipeline-%s [%s]", pipelineId, action.getName());
                action.setName(actionName);
                if (actionNames.contains(actionName)) {
                    duplicatedActionNames.add(actionName);
                }
                actionNames.add(actionName);
            }
        }
        // 检查，不能存在重复的Action Name
        checkArgument(
                duplicatedActionNames.isEmpty(),
                "Action name is duplicated: " + duplicatedActionNames);

        return pipelines;
    }
}
