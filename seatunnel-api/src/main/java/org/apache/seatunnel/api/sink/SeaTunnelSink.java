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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.common.PluginIdentifierInterface;
import org.apache.seatunnel.api.common.SeaTunnelPluginLifeCycle;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelJobAware;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * The SeaTunnel sink interface, developer should implement this class when create a sink connector.
 *
 * @param <IN> The data class by sink accept. Only support {@link
 *     org.apache.seatunnel.api.table.type.SeaTunnelRow} at now.
 * @param <StateT> The state should be saved when job execute, this class should implement interface
 *     {@link Serializable}.
 * @param <CommitInfoT> The commit message class return by {@link SinkWriter#prepareCommit()}, then
 *     {@link SinkCommitter} or {@link SinkAggregatedCommitter} and handle it, this class should
 *     implement interface {@link Serializable}.
 * @param <AggregatedCommitInfoT> The aggregated commit message class, combine by {@link
 *     CommitInfoT}. {@link SinkAggregatedCommitter} handle it, this class should implement
 *     interface {@link Serializable}.
 */
public interface SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT>
        extends Serializable,
                PluginIdentifierInterface,
                SeaTunnelPluginLifeCycle,
                SeaTunnelJobAware {

    /**
     * Set the row type info of sink row data. This method will be automatically called by
     * translation.
     *
     * @deprecated instead by {@link org.apache.seatunnel.api.table.factory.Factory}
     * @param seaTunnelRowType The row type info of sink.
     */
    @Deprecated
    default void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        throw new UnsupportedOperationException("setTypeInfo method is not supported");
    }

    /**
     * Get the data type of the records consumed by this sink.
     *
     * @deprecated instead by {@link org.apache.seatunnel.api.table.factory.Factory}
     * @return SeaTunnel data type.
     */
    @Deprecated
    default SeaTunnelDataType<IN> getConsumedType() {
        throw new UnsupportedOperationException("getConsumedType method is not supported");
    }

    /**
     * This method will be called to creat {@link SinkWriter}
     * 创建Writer实例，与Source类似，数据的实际写入是由Writer来写入。
     * @param context The sink context
     * @return Return sink writer instance
     * @throws IOException throws IOException when createWriter failed.
     */
    SinkWriter<IN, CommitInfoT, StateT> createWriter(SinkWriter.Context context) throws IOException;

    /**
     * 恢复 SinkWriter，Writer是真正执行数据写入的类
     * @param context
     * @param states
     * @return
     * @throws IOException
     */
    default SinkWriter<IN, CommitInfoT, StateT> restoreWriter(
            SinkWriter.Context context, List<StateT> states) throws IOException {
        return createWriter(context);
    }

    /**
     * Get {@link StateT} serializer. So that {@link StateT} can be transferred across processes
     *
     * @return Serializer of {@link StateT}
     */
    default Optional<Serializer<StateT>> getWriterStateSerializer() {
        return Optional.empty();
    }

    /**
     * This method will be called to create {@link SinkCommitter}
     *  可选，在需要二阶段提交时，创建一个SinkCommitter，由SinkCommitter来完成二阶段提交, 此方式也不再推荐, 推荐使用
     * @return Return sink committer instance
     * @throws IOException throws IOException when createCommitter failed.
     */
    default Optional<SinkCommitter<CommitInfoT>> createCommitter() throws IOException {
        return Optional.empty();
    }

    /**
     * Get {@link CommitInfoT} serializer. So that {@link CommitInfoT} can be transferred across
     * processes
     *
     * @return Serializer of {@link CommitInfoT}
     */
    default Optional<Serializer<CommitInfoT>> getCommitInfoSerializer() {
        return Optional.empty();
    }

    /**
     * This method will be called to create {@link SinkAggregatedCommitter}
     * 可选，与SinkCommitter类似，都是在提交阶段进行二阶段提交使用。
     * SinkAggregatedCommitter是单一实例去执行，不会存在多实例，将所有的提交任务集中到一个地方执行。所以如果连接器需要二阶段提交, 推荐使用createAggregatedCommitter()来创建
     *
     * @return Return sink aggregated committer instance
     * @throws IOException throws IOException when createAggregatedCommitter failed.
     */
    default Optional<SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT>>
            createAggregatedCommitter() throws IOException {
        return Optional.empty();
    }

    /**
     * Get {@link AggregatedCommitInfoT} serializer. So that {@link AggregatedCommitInfoT} can be
     * transferred across processes
     *
     * @return Serializer of {@link AggregatedCommitInfoT}
     */
    default Optional<Serializer<AggregatedCommitInfoT>> getAggregatedCommitInfoSerializer() {
        return Optional.empty();
    }

    /**
     * Get the catalog table of the sink.
     *
     * @return Optional of catalog table.
     */
    default Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.empty();
    }
}
