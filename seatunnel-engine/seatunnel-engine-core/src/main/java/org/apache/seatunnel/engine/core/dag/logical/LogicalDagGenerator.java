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

package org.apache.seatunnel.engine.core.dag.logical;

import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalDagGenerator {
    private static final ILogger LOGGER = Logger.getLogger(LogicalDagGenerator.class);
    private List<Action> actions;
    private JobConfig jobConfig;
    private IdGenerator idGenerator;
    private boolean isStartWithSavePoint;

    private final Map<Long, LogicalVertex> logicalVertexMap = new LinkedHashMap<>();

    /**
     * key: input vertex id; <br>
     * value: target vertices id;
     */
    private final Map<Long, LinkedHashSet<Long>> inputVerticesMap = new LinkedHashMap<>();

    public LogicalDagGenerator(
            @NonNull List<Action> actions,
            @NonNull JobConfig jobConfig,
            @NonNull IdGenerator idGenerator) {
        this(actions, jobConfig, idGenerator, false);
    }

    public LogicalDagGenerator(
            @NonNull List<Action> actions,
            @NonNull JobConfig jobConfig,
            @NonNull IdGenerator idGenerator,
            boolean isStartWithSavePoint) {
        this.actions = actions;
        this.jobConfig = jobConfig;
        this.idGenerator = idGenerator;
        this.isStartWithSavePoint = isStartWithSavePoint;
        if (actions.isEmpty()) {
            throw new IllegalStateException("No actions define in the job. Cannot execute.");
        }
    }

    /**
     * 逻辑计划生成
     * @return
     */
    public LogicalDag generate() {
        // 根据action来生成节点信息
        actions.forEach(this::createLogicalVertex);
        // 创建边
        Set<LogicalEdge> logicalEdges = createLogicalEdges();
        // 构建LogicalDag对象，并将解析的值设置到相应属性中
        LogicalDag logicalDag = new LogicalDag(jobConfig, idGenerator);
        logicalDag.getEdges().addAll(logicalEdges);
        logicalDag.getLogicalVertexMap().putAll(logicalVertexMap);
        logicalDag.setStartWithSavePoint(isStartWithSavePoint);
        return logicalDag;
    }

    private void createLogicalVertex(Action action) {
        // 获取当前action的id,判断当map中已经存在则返回
        final Long logicalVertexId = action.getId();
        if (logicalVertexMap.containsKey(logicalVertexId)) {
            return;
        }
        // 对上游的依赖进行循环创建
        // map对象的存储结构为：
        // 当前节点的id为key
        // value为一个list，存储下游使用到该节点的id编号
        // connection vertices info
        action.getUpstream()
                .forEach(
                        inputAction -> {
                            createLogicalVertex(inputAction);
                            inputVerticesMap
                                    .computeIfAbsent(
                                            inputAction.getId(), id -> new LinkedHashSet<>())
                                    .add(logicalVertexId);
                        });
        // 最后创建当前节点的信息
        final LogicalVertex logicalVertex =
                new LogicalVertex(logicalVertexId, action, action.getParallelism());
        // 注意这里有两个map
        // 一个为inputVerticesMap，一个为logicalVertexMap
        // inputVerticesMap中存储了节点之间的关系
        // logicalVertexMap存储了节点编号与节点的关系
        logicalVertexMap.put(logicalVertexId, logicalVertex);
    }

    private Set<LogicalEdge> createLogicalEdges() {
        return inputVerticesMap.entrySet().stream()
                .map(
                        entry ->
                                entry.getValue().stream()
                                        .map(
                                                targetId ->
                                                        new LogicalEdge(
                                                                logicalVertexMap.get(
                                                                        entry.getKey()),
                                                                logicalVertexMap.get(targetId)))
                                        .collect(Collectors.toList()))
                .flatMap(Collection::stream)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
