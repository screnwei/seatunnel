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

package org.apache.seatunnel.engine.core.parse;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.api.sink.SaveModeExecuteLocation;
import org.apache.seatunnel.api.sink.SaveModeExecuteWrapper;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceCheckpoint;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkConfig;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.DEFAULT_ID;
import static org.apache.seatunnel.engine.core.parse.ConfigParserUtil.getFactoryId;
import static org.apache.seatunnel.engine.core.parse.ConfigParserUtil.getInputIds;

@Slf4j
public class MultipleTableJobConfigParser {

    private final IdGenerator idGenerator;
    private final JobConfig jobConfig;

    private final List<URL> commonPluginJars;
    private final Config seaTunnelJobConfig;

    private final ReadonlyConfig envOptions;

    private final JobConfigParser fallbackParser;
    private final boolean isStartWithSavePoint;
    private final List<JobPipelineCheckpointData> pipelineCheckpoints;

    public MultipleTableJobConfigParser(
            String jobDefineFilePath, IdGenerator idGenerator, JobConfig jobConfig) {
        this(jobDefineFilePath, idGenerator, jobConfig, Collections.emptyList(), false);
    }

    public MultipleTableJobConfigParser(
            Config seaTunnelJobConfig, IdGenerator idGenerator, JobConfig jobConfig) {
        this(
                seaTunnelJobConfig,
                idGenerator,
                jobConfig,
                Collections.emptyList(),
                false,
                Collections.emptyList());
    }

    public MultipleTableJobConfigParser(
            String jobDefineFilePath,
            IdGenerator idGenerator,
            JobConfig jobConfig,
            List<URL> commonPluginJars,
            boolean isStartWithSavePoint) {
        this(
                jobDefineFilePath,
                null,
                idGenerator,
                jobConfig,
                commonPluginJars,
                isStartWithSavePoint,
                Collections.emptyList());
    }

    public MultipleTableJobConfigParser(
            String jobDefineFilePath,
            List<String> variables,
            IdGenerator idGenerator,
            JobConfig jobConfig,
            List<URL> commonPluginJars,
            boolean isStartWithSavePoint,
            List<JobPipelineCheckpointData> pipelineCheckpoints) {
        this.idGenerator = idGenerator;
        this.jobConfig = jobConfig;
        this.commonPluginJars = commonPluginJars;
        this.isStartWithSavePoint = isStartWithSavePoint;
        this.seaTunnelJobConfig = ConfigBuilder.of(Paths.get(jobDefineFilePath), variables);
        this.envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        this.fallbackParser =
                new JobConfigParser(idGenerator, commonPluginJars, this, isStartWithSavePoint);
        this.pipelineCheckpoints = pipelineCheckpoints;
    }

    public MultipleTableJobConfigParser(
            Config seaTunnelJobConfig,
            IdGenerator idGenerator,
            JobConfig jobConfig,
            List<URL> commonPluginJars,
            boolean isStartWithSavePoint,
            List<JobPipelineCheckpointData> pipelineCheckpoints) {
        this.idGenerator = idGenerator;
        this.jobConfig = jobConfig;
        this.commonPluginJars = commonPluginJars;
        this.isStartWithSavePoint = isStartWithSavePoint;
        this.seaTunnelJobConfig = seaTunnelJobConfig;
        this.envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        this.fallbackParser =
                new JobConfigParser(idGenerator, commonPluginJars, this, isStartWithSavePoint);
        this.pipelineCheckpoints = pipelineCheckpoints;
    }

    public ImmutablePair<List<Action>, Set<URL>> parse(ClassLoaderService classLoaderService) {
        // 将配置文件中的 env.jars添加到 commonJars中
        this.fillJobConfigAndCommonJars();
        // 从配置文件中，将source，transform，sink的配置分别读取处理
        List<? extends Config> sourceConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "source", Collections.emptyList());
        List<? extends Config> transformConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "transform", Collections.emptyList());
        List<? extends Config> sinkConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "sink", Collections.emptyList());

        // 获取连接器的jar包地址
        List<URL> sourceConnectorJars = getConnectorJarList(sourceConfigs, PluginType.SOURCE);
        List<URL> transformConnectorJars =
                getConnectorJarList(transformConfigs, PluginType.TRANSFORM);
        List<URL> sinkConnectorJars = getConnectorJarList(sinkConfigs, PluginType.SINK);
        ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();

        // source and transform use the same classloader
        List<URL> sourceJars =
                Stream.of(sourceConnectorJars, transformConnectorJars)
                        .flatMap(Collection::stream)
                        .distinct()
                        .collect(Collectors.toList());
        ClassLoader sourceAndTransformClassLoader =
                getClassLoader(classLoaderService, parentClassLoader, sourceJars);
        ClassLoader sinkClassLoader =
                getClassLoader(classLoaderService, parentClassLoader, sinkConnectorJars);

        try {
            Thread.currentThread().setContextClassLoader(sourceAndTransformClassLoader);
            // 检查DAG里面是否构成环，避免后续的构建过程陷入循环
            ConfigParserUtil.checkGraph(sourceConfigs, transformConfigs, sinkConfigs);
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap =
                    new LinkedHashMap<>();

            log.info("start generating all sources.");
            if (isStartWithSavePoint
                    && pipelineCheckpoints != null
                    && !pipelineCheckpoints.isEmpty()) {
                Preconditions.checkState(
                        sourceConfigs.size() == pipelineCheckpoints.size(),
                        "The number of source configurations and pipeline checkpoints must be equal.");
            }
            for (int configIndex = 0; configIndex < sourceConfigs.size(); configIndex++) {
                Config sourceConfig = sourceConfigs.get(configIndex);
                // parseSource方法为真正生成source的方法
                // 返回值为2元组，第一个值为 当前source生成的表名称
                // 第二个值为 CatalogTable和Action的二元组列表
                // 由于SeaTunnel Source支持读取多表，所以第二个值为列表
                Tuple2<String, List<Tuple2<CatalogTable, Action>>> tuple2 =
                        parseSource(configIndex, sourceConfig, sourceAndTransformClassLoader);
                tableWithActionMap.put(tuple2._1(), tuple2._2());
            }

            log.info("start generating all transforms.");
            // 这里将上面的 tableWithActionMap传递了进去，所以不需要返回值
            parseTransforms(transformConfigs, sourceAndTransformClassLoader, tableWithActionMap);

            Thread.currentThread().setContextClassLoader(sinkClassLoader);
            log.info("start generating all sinks.");
            List<Action> sinkActions = new ArrayList<>();
            for (int configIndex = 0; configIndex < sinkConfigs.size(); configIndex++) {
                Config sinkConfig = sinkConfigs.get(configIndex);
                // parseSink方法来生成sink
                // 同样，传递了tableWithActionMap
                sinkActions.addAll(
                        parseSink(configIndex, sinkConfig, sinkClassLoader, tableWithActionMap));
            }
            Set<URL> factoryUrls = getUsedFactoryUrls(sinkActions);
            return new ImmutablePair<>(sinkActions, factoryUrls);
        } finally {
            // 将当前线程的类加载器切换为原来的类加载器
            Thread.currentThread().setContextClassLoader(parentClassLoader);
            if (classLoaderService != null) {
                classLoaderService.releaseClassLoader(
                        Long.parseLong(jobConfig.getJobContext().getJobId()), sourceJars);
                classLoaderService.releaseClassLoader(
                        Long.parseLong(jobConfig.getJobContext().getJobId()), sinkConnectorJars);
            }
        }
    }

    private ClassLoader getClassLoader(
            ClassLoaderService classLoaderService,
            ClassLoader parentClassLoader,
            List<URL> connectorJars) {
        ClassLoader classLoader;
        if (classLoaderService == null) {
            classLoader = new SeaTunnelChildFirstClassLoader(connectorJars, parentClassLoader);
        } else {
            classLoader =
                    classLoaderService.getClassLoader(
                            Long.parseLong(jobConfig.getJobContext().getJobId()), connectorJars);
        }
        return classLoader;
    }

    public Set<URL> getUsedFactoryUrls(List<Action> sinkActions) {
        Set<URL> urls = new HashSet<>();
        fillUsedFactoryUrls(sinkActions, urls);
        return urls;
    }

    private List<URL> getConnectorJarList(List<? extends Config> configs, PluginType type) {
        List<PluginIdentifier> factoryIds =
                configs.stream()
                        .map(ConfigParserUtil::getFactoryId)
                        .map(
                                factory ->
                                        PluginIdentifier.of(
                                                CollectionConstants.SEATUNNEL_PLUGIN,
                                                type.getType(),
                                                factory))
                        .collect(Collectors.toList());
        List<URL> jarPaths = new ArrayList<>();
        jarPaths.addAll(new SeaTunnelSinkPluginDiscovery().getPluginJarPaths(factoryIds));
        jarPaths.addAll(commonPluginJars);
        return jarPaths;
    }

    private void fillUsedFactoryUrls(List<Action> actions, Set<URL> result) {
        actions.forEach(
                action -> {
                    result.addAll(action.getJarUrls());
                    if (!action.getUpstream().isEmpty()) {
                        fillUsedFactoryUrls(action.getUpstream(), result);
                    }
                });
    }

    private void fillJobConfigAndCommonJars() {
        jobConfig.getJobContext().setJobMode(envOptions.get(EnvCommonOptions.JOB_MODE));
        if (StringUtils.isEmpty(jobConfig.getName())
                || jobConfig.getName().equals(Constants.LOGO)
                || jobConfig.getName().equals(EnvCommonOptions.JOB_NAME.defaultValue())) {
            jobConfig.setName(envOptions.get(EnvCommonOptions.JOB_NAME));
        }
        jobConfig.getEnvOptions().putAll(envOptions.getSourceMap());
        this.commonPluginJars.addAll(
                new ArrayList<>(
                        Common.getThirdPartyJars(
                                        jobConfig
                                                .getEnvOptions()
                                                .getOrDefault(EnvCommonOptions.JARS.key(), "")
                                                .toString())
                                .stream()
                                .map(Path::toUri)
                                .map(
                                        uri -> {
                                            try {
                                                return uri.toURL();
                                            } catch (MalformedURLException e) {
                                                throw new SeaTunnelEngineException(
                                                        "the uri of jar illegal:" + uri, e);
                                            }
                                        })
                                .collect(Collectors.toList())));
        log.info("add common jar in plugins :{}", commonPluginJars);
    }

    private static <T extends Factory> boolean isFallback(
            ClassLoader classLoader,
            Class<T> factoryClass,
            String factoryId,
            Consumer<T> virtualCreator) {
        Optional<T> factory =
                FactoryUtil.discoverOptionalFactory(classLoader, factoryClass, factoryId);
        if (!factory.isPresent()) {
            return true;
        }
        try {
            virtualCreator.accept(factory.get());
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException
                    && "The Factory has not been implemented and the deprecated Plugin will be used."
                            .equals(e.getMessage())) {
                log.warn(
                        "The Factory has not been implemented and the deprecated Plugin will be used.");
                return true;
            }
            log.debug(ExceptionUtils.getMessage(e));
        }
        return false;
    }

    private int getParallelism(ReadonlyConfig config) {
        return Math.max(
                1,
                config.getOptional(CommonOptions.PARALLELISM)
                        .orElse(envOptions.get(CommonOptions.PARALLELISM)));
    }

    public Tuple2<String, List<Tuple2<CatalogTable, Action>>> parseSource(
            int configIndex, Config sourceConfig, ClassLoader classLoader) {
        final ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sourceConfig);
        // factoryId就是我们配置里面的 source名称，例如 FakeSource， Jdbc
        final String factoryId = getFactoryId(readonlyConfig);
        // 获取当前数据源生成的 表 名称，注意这里的表可能并不对应一个表
        // 由于 seatunnel source支持多表读取，那么这里就会出现一对多的关系
        final String tableId =
                readonlyConfig.getOptional(CommonOptions.PLUGIN_OUTPUT).orElse(DEFAULT_ID);
        // 获取并行度
        final int parallelism = getParallelism(readonlyConfig);

        // 这个地方是由于某些Source还不支持通过Factory工厂来构建，所以会有两种构建方法
        // 后续当所有连接器都支持通过工厂来创建后，这里的代码会被删除掉，所以这次忽略掉这部分代码
        // 方法内部是查询是否有相应的工厂类，相应的工厂类不存在时返回 true，不存在时返回false
        boolean fallback =
                isFallback(
                        classLoader,
                        TableSourceFactory.class,
                        factoryId,
                        (factory) -> factory.createSource(null));

        if (fallback) {
            Tuple2<CatalogTable, Action> tuple =
                    fallbackParser.parseSource(sourceConfig, jobConfig, tableId, parallelism);
            return new Tuple2<>(tableId, Collections.singletonList(tuple));
        }

        // 通过FactoryUtil来创建Source
        // 返回对象为 SeaTunnelSource实例，以及List<CatalogTable>
        // 这里会创建我们同步任务中Source的实例，catalogtable列表表示这个数据源读取的表的表结构等信息
        Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>> tuple2;
        if (isStartWithSavePoint && pipelineCheckpoints != null && !pipelineCheckpoints.isEmpty()) {
            ChangeStreamTableSourceCheckpoint checkpoint =
                    getSourceCheckpoint(configIndex, factoryId);
            tuple2 =
                    FactoryUtil.restoreAndPrepareSource(
                            readonlyConfig, classLoader, factoryId, checkpoint);
        } else {
            tuple2 = FactoryUtil.createAndPrepareSource(readonlyConfig, classLoader, factoryId);
        }

        // 获取当前source connector的jar包
        Set<URL> factoryUrls = new HashSet<>();
        factoryUrls.addAll(getSourcePluginJarPaths(sourceConfig));

        List<Tuple2<CatalogTable, Action>> actions = new ArrayList<>();
        long id = idGenerator.getNextId();
        String actionName = JobConfigParser.createSourceActionName(configIndex, factoryId);
        SeaTunnelSource<Object, SourceSplit, Serializable> source = tuple2._1();
        source.setJobContext(jobConfig.getJobContext());
        PluginUtil.ensureJobModeMatch(jobConfig.getJobContext(), source);
        // 构建 SourceAction
        SourceAction<Object, SourceSplit, Serializable> action =
                new SourceAction<>(id, actionName, tuple2._1(), factoryUrls, new HashSet<>());
        action.setParallelism(parallelism);
        for (CatalogTable catalogTable : tuple2._2()) {
            actions.add(new Tuple2<>(catalogTable, action));
        }
        return new Tuple2<>(tableId, actions);
    }

    public void parseTransforms(
            List<? extends Config> transformConfigs,
            ClassLoader classLoader,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
        if (CollectionUtils.isEmpty(transformConfigs) || transformConfigs.isEmpty()) {
            return;
        }
        Queue<Config> configList = new LinkedList<>(transformConfigs);
        int index = 0;
        while (!configList.isEmpty()) {
            parseTransform(index++, configList, classLoader, tableWithActionMap);
        }
    }

    private void parseTransform(
            int index,
            Queue<Config> transforms,
            ClassLoader classLoader,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
        Config config = transforms.poll();
        final ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        final String factoryId = getFactoryId(readonlyConfig);
        // get jar urls
        Set<URL> jarUrls = new HashSet<>();
        jarUrls.addAll(getTransformPluginJarPaths(config));
        final List<String> inputIds = getInputIds(readonlyConfig);

        // inputIds为source_table_name，根据这个值找到所依赖的上游source
        List<Tuple2<CatalogTable, Action>> inputs =
                inputIds.stream()
                        .map(tableWithActionMap::get)
                        .filter(Objects::nonNull)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());

        // inputs为空，表明当前Transform节点找不到任何上游的节点
        if (inputs.isEmpty()) {
            if (transforms.isEmpty()) {
                // Tolerates incorrect configuration of simple graph
                // 未设置source_table_name，设置结果与之前不对应并且只有一个transform时
                // 把最后一个source作为这个transform的上游表
                inputs = findLast(tableWithActionMap);
            } else {
                // The previous transform has not been created
                // 所依赖的transform可能还没有创建，将本次的transform再放回队列中，后续再进行解析
                transforms.offer(config);
                return;
            }
        }

        // 这次transform结果产生的表名称
        final String tableId =
                readonlyConfig.getOptional(CommonOptions.PLUGIN_OUTPUT).orElse(DEFAULT_ID);

        // 获取上游source的Action
        Set<Action> inputActions =
                inputs.stream()
                        .map(Tuple2::_2)
                        .collect(Collectors.toCollection(LinkedHashSet::new));

        LinkedHashSet<CatalogTable> catalogTables =
                inputs.stream()
                        .map(Tuple2::_1)
                        .collect(Collectors.toCollection(LinkedHashSet::new));

        // 验证所依赖的多个上游，是否产生的表结构都相同，只有所有的表结构都相同才能进入一个transform来处理
        checkProducedTypeEquals(inputActions);
        int spareParallelism = inputs.get(0)._2().getParallelism();

        // 设置并行度
        int parallelism =
                readonlyConfig.getOptional(CommonOptions.PARALLELISM).orElse(spareParallelism);
        SeaTunnelTransform<?> transform =
                FactoryUtil.createAndPrepareMultiTableTransform(
                        new ArrayList<>(catalogTables), readonlyConfig, classLoader, factoryId);

        transform.setJobContext(jobConfig.getJobContext());
        long id = idGenerator.getNextId();
        String actionName = JobConfigParser.createTransformActionName(index, factoryId);

        // 封装成Action
        TransformAction transformAction =
                new TransformAction(
                        id,
                        actionName,
                        new ArrayList<>(inputActions),
                        transform,
                        jarUrls,
                        new HashSet<>());
        transformAction.setParallelism(parallelism);

        List<Tuple2<CatalogTable, Action>> actions = new ArrayList<>();
        List<CatalogTable> producedCatalogTables = transform.getProducedCatalogTables();

        for (CatalogTable catalogTable : producedCatalogTables) {
            actions.add(new Tuple2<>(catalogTable, transformAction));
        }

        // 放入到map中，此时map里面存储了source和transform
        // 以每个节点产生的表结构为key，action作为value
        tableWithActionMap.put(tableId, actions);
    }

    public static SeaTunnelDataType<?> getProducedType(Action action) {
        if (action instanceof SourceAction) {
            try {
                return ((SourceAction<?, ?, ?>) action)
                        .getSource()
                        .getProducedCatalogTables()
                        .get(0)
                        .getSeaTunnelRowType();
            } catch (UnsupportedOperationException e) {
                // TODO remove it when all connector use `getProducedCatalogTables`
                return ((SourceAction<?, ?, ?>) action).getSource().getProducedType();
            }
        } else if (action instanceof TransformAction) {
            return ((TransformAction) action)
                    .getTransform()
                    .getProducedCatalogTable()
                    .getSeaTunnelRowType();
        }
        throw new UnsupportedOperationException();
    }

    public static void checkProducedTypeEquals(Set<Action> inputActions) {
        SeaTunnelDataType<?> expectedType = getProducedType(new ArrayList<>(inputActions).get(0));
        for (Action action : inputActions) {
            SeaTunnelDataType<?> producedType = getProducedType(action);
            if (!expectedType.equals(producedType)) {
                throw new JobDefineCheckException(
                        "Transform/Sink don't support processing data with two different structures.");
            }
        }
    }

    @Deprecated
    private static <T> T findLast(LinkedHashMap<?, T> map) {
        int size = map.size();
        int i = 1;
        for (T value : map.values()) {
            if (i == size) {
                return value;
            }
            i++;
        }
        // never execution
        return null;
    }

    public List<SinkAction<?, ?, ?, ?>> parseSink(
            int configIndex,
            Config sinkConfig,
            ClassLoader classLoader,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sinkConfig);
        String factoryId = getFactoryId(readonlyConfig);
        // 获取当前sink节点依赖的上游节点
        List<String> inputIds = getInputIds(readonlyConfig);

        List<List<Tuple2<CatalogTable, Action>>> inputVertices =
                inputIds.stream()
                        .map(tableWithActionMap::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        // 当sink节点找不到上游节点时，找到最后一个节点信息作为上游节点
        // 这里与transform不一样的地方是，不会再等其他sink节点初始化完成，因为sink节点不可能依赖与其他sink节点
        if (inputVertices.isEmpty()) {
            // Tolerates incorrect configuration of simple graph
            inputVertices = Collections.singletonList(findLast(tableWithActionMap));
        } else if (inputVertices.size() > 1) {
            for (List<Tuple2<CatalogTable, Action>> inputVertex : inputVertices) {
                if (inputVertex.size() > 1) {
                    // 当一个sink节点即有多个上游节点，且某个上游节点还会产生多表时抛出异常
                    // sink可以支持多个数据源，或者单个数据源下产生多表，不能同时支持多个数据源，且某个数据源下存在多表
                    throw new JobDefineCheckException(
                            "Sink don't support simultaneous writing of data from multi-table source and other sources.");
                }
            }
        }

        //对老代码的兼容
        boolean fallback =
                isFallback(
                        classLoader,
                        TableSinkFactory.class,
                        factoryId,
                        (factory) -> factory.createSink(null));
        if (fallback) {
            return fallbackParser.parseSinks(configIndex, inputVertices, sinkConfig, jobConfig);
        }

        // get jar urls
        // 获取sink的连接器jar包
        Set<URL> jarUrls = new HashSet<>();
        jarUrls.addAll(getSinkPluginJarPaths(sinkConfig));
        List<SinkAction<?, ?, ?, ?>> sinkActions = new ArrayList<>();

        // union
        // 多个数据源的情况
        if (inputVertices.size() > 1) {
            Set<Action> inputActions =
                    inputVertices.stream()
                            .flatMap(Collection::stream)
                            .map(Tuple2::_2)
                            .collect(Collectors.toCollection(LinkedHashSet::new));
            // 检查多个上游数据源产生的表结构是否一致
            checkProducedTypeEquals(inputActions);
            Tuple2<CatalogTable, Action> inputActionSample = inputVertices.get(0).get(0);
            // 创建sinkAction
            SinkAction<?, ?, ?, ?> sinkAction =
                    createSinkAction(
                            inputActionSample._1(),
                            inputActions,
                            readonlyConfig,
                            classLoader,
                            jarUrls,
                            new HashSet<>(),
                            factoryId,
                            inputActionSample._2().getParallelism(),
                            configIndex);
            sinkActions.add(sinkAction);
            return sinkActions;
        }

        // TODO move it into tryGenerateMultiTableSink when we don't support sink template
        // sink template
        // 此时只有一个数据源，且此数据源下可能会产生多表，循环创建sinkAction
        for (Tuple2<CatalogTable, Action> tuple : inputVertices.get(0)) {
            SinkAction<?, ?, ?, ?> sinkAction =
                    createSinkAction(
                            tuple._1(),
                            Collections.singleton(tuple._2()),
                            readonlyConfig,
                            classLoader,
                            jarUrls,
                            new HashSet<>(),
                            factoryId,
                            tuple._2().getParallelism(),
                            configIndex);
            sinkActions.add(sinkAction);
        }
        Optional<SinkAction<?, ?, ?, ?>> multiTableSink =
                tryGenerateMultiTableSink(
                        sinkActions, readonlyConfig, classLoader, factoryId, configIndex);
        // 最终会将所创建的sink action作为返回值返回
        return multiTableSink
                .<List<SinkAction<?, ?, ?, ?>>>map(Collections::singletonList)
                .orElse(sinkActions);
    }

    private Optional<SinkAction<?, ?, ?, ?>> tryGenerateMultiTableSink(
            List<SinkAction<?, ?, ?, ?>> sinkActions,
            ReadonlyConfig options,
            ClassLoader classLoader,
            String factoryId,
            int configIndex) {
        if (sinkActions.stream()
                .anyMatch(action -> !(action.getSink() instanceof SupportMultiTableSink))) {
            log.info("Unsupported multi table sink api, rollback to sink template");
            return Optional.empty();
        }
        Map<String, SeaTunnelSink> sinks = new HashMap<>();
        Set<URL> jars =
                sinkActions.stream()
                        .flatMap(a -> a.getJarUrls().stream())
                        .collect(Collectors.toSet());
        sinkActions.forEach(
                action -> {
                    SeaTunnelSink sink = action.getSink();
                    String tableId = action.getConfig().getMultipleRowTableId();
                    sinks.put(tableId, sink);
                });
        SeaTunnelSink<?, ?, ?, ?> sink =
                FactoryUtil.createMultiTableSink(sinks, options, classLoader);
        String actionName =
                JobConfigParser.createSinkActionName(configIndex, factoryId, "MultiTableSink");
        SinkAction<?, ?, ?, ?> multiTableAction =
                new SinkAction<>(
                        idGenerator.getNextId(),
                        actionName,
                        sinkActions.get(0).getUpstream(),
                        sink,
                        jars,
                        new HashSet<>());
        multiTableAction.setParallelism(sinkActions.get(0).getParallelism());
        return Optional.of(multiTableAction);
    }

    private SinkAction<?, ?, ?, ?> createSinkAction(
            CatalogTable catalogTable,
            Set<Action> inputActions,
            ReadonlyConfig readonlyConfig,
            ClassLoader classLoader,
            Set<URL> factoryUrls,
            Set<ConnectorJarIdentifier> connectorJarIdentifiers,
            String factoryId,
            int parallelism,
            int configIndex) {

        // 使用工厂类创建sink
        SeaTunnelSink<?, ?, ?, ?> sink =
                FactoryUtil.createAndPrepareSink(
                        catalogTable, readonlyConfig, classLoader, factoryId);
        sink.setJobContext(jobConfig.getJobContext());
        SinkConfig actionConfig =
                new SinkConfig(catalogTable.getTableId().toTablePath().toString());
        long id = idGenerator.getNextId();
        String actionName =
                JobConfigParser.createSinkActionName(
                        configIndex, factoryId, actionConfig.getMultipleRowTableId());

        // 创建sinkAction
        SinkAction<?, ?, ?, ?> sinkAction =
                new SinkAction<>(
                        id,
                        actionName,
                        new ArrayList<>(inputActions),
                        sink,
                        factoryUrls,
                        connectorJarIdentifiers,
                        actionConfig);
        if (!isStartWithSavePoint) {
            // 这里需要注意，当非从savepoint启动时，会进行savemode的处理
            handleSaveMode(sink);
        }
        sinkAction.setParallelism(parallelism);
        return sinkAction;
    }

    public void handleSaveMode(SeaTunnelSink<?, ?, ?, ?> sink) {
        // 当sink类支持了savemode特性时，会进行savemode处理
        // 例如删除表，重建表，报错等
        if (SupportSaveMode.class.isAssignableFrom(sink.getClass())) {
            SupportSaveMode saveModeSink = (SupportSaveMode) sink;
            // 当 设置savemode在client端执行时，会在client端去做这些事
            // 我们之前出现过一个错误是当在客户端执行完毕后，到集群后任务执行出错，卡在scheduling的状态
            // 导致数据被清空后没有及时写入
            // 以及需要注意这个地方执行的机器到sink集群的网络是否能够连通，推荐将这个行为放到server端执行
            if (envOptions
                    .get(EnvCommonOptions.SAVEMODE_EXECUTE_LOCATION)
                    .equals(SaveModeExecuteLocation.CLIENT)) {
                log.warn(
                        "SaveMode execute location on CLIENT is deprecated, please use CLUSTER instead.");
                Optional<SaveModeHandler> saveModeHandler = saveModeSink.getSaveModeHandler();
                if (saveModeHandler.isPresent()) {
                    try (SaveModeHandler handler = saveModeHandler.get()) {
                        handler.open();
                        new SaveModeExecuteWrapper(handler).execute();
                    } catch (Exception e) {
                        throw new SeaTunnelRuntimeException(HANDLE_SAVE_MODE_FAILED, e);
                    }
                }
            }
        }
    }

    private List<URL> getSourcePluginJarPaths(Config sourceConfig) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        CollectionConstants.SOURCE_PLUGIN,
                        sourceConfig.getString(CollectionConstants.PLUGIN_NAME));
        List<URL> pluginJarPaths =
                sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
        return pluginJarPaths;
    }

    private List<URL> getTransformPluginJarPaths(Config transformConfig) {
        SeaTunnelTransformPluginDiscovery transformPluginDiscovery =
                new SeaTunnelTransformPluginDiscovery();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        CollectionConstants.TRANSFORM_PLUGIN,
                        transformConfig.getString(CollectionConstants.PLUGIN_NAME));
        List<URL> pluginJarPaths =
                transformPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
        return pluginJarPaths;
    }

    private List<URL> getSinkPluginJarPaths(Config sinkConfig) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        CollectionConstants.SINK_PLUGIN,
                        sinkConfig.getString(CollectionConstants.PLUGIN_NAME));
        List<URL> pluginJarPaths =
                sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
        return pluginJarPaths;
    }

    private ChangeStreamTableSourceCheckpoint getSourceCheckpoint(
            int sourceConfigIndex, String sourceFactoryId) {
        String sourceActionName =
                JobConfigParser.createSourceActionName(sourceConfigIndex, sourceFactoryId);
        JobPipelineCheckpointData pipelineCheckpointData =
                pipelineCheckpoints.get(sourceConfigIndex);
        Preconditions.checkArgument(
                pipelineCheckpointData.getPipelineId() == sourceConfigIndex + 1,
                String.format(
                        "The pipeline id in the checkpoint data is %d, but the config index is %d.",
                        pipelineCheckpointData.getPipelineId(), sourceConfigIndex + 1));

        List<JobPipelineCheckpointData.ActionState> sourceCheckpointData =
                pipelineCheckpointData.getTaskStates().entrySet().stream()
                        .filter(entry -> entry.getKey().contains(sourceActionName))
                        .map(e -> e.getValue())
                        .collect(Collectors.toList());
        Preconditions.checkArgument(
                sourceCheckpointData.size() == 1,
                String.format(
                        "The source action name %s is not found in the checkpoint keys %s.",
                        sourceActionName, pipelineCheckpointData.getTaskStates().keySet()));

        byte[] coordinatorState = sourceCheckpointData.get(0).getCoordinatorState().get(0);
        List<List<byte[]>> subtaskState =
                sourceCheckpointData.get(0).getSubtaskState().stream()
                        .flatMap(
                                (Function<
                                                JobPipelineCheckpointData.ActionSubtaskState,
                                                Stream<List<byte[]>>>)
                                        state ->
                                                state == null
                                                        ? Stream.of(Collections.emptyList())
                                                        : Stream.of(state.getState()))
                        .collect(Collectors.toList());
        return new ChangeStreamTableSourceCheckpoint(coordinatorState, subtaskState);
    }
}
