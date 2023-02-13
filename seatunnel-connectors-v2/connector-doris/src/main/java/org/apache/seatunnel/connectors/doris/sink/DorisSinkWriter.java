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

package org.apache.seatunnel.connectors.doris.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.doris.client.DorisSinkManager;
import org.apache.seatunnel.connectors.doris.config.SinkConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.serialize.DorisColumnRowSerialize;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class DorisSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final DorisColumnRowSerialize dorisColumnRowSerialize;
    private final DorisSinkManager manager;

    public DorisSinkWriter(Config pluginConfig, SeaTunnelRowType seaTunnelRowType) {
        SinkConfig sinkConfig = SinkConfig.loadConfig(pluginConfig);
        List<String> fieldNames =
                Arrays.stream(seaTunnelRowType.getFieldNames()).collect(Collectors.toList());
        this.dorisColumnRowSerialize =
                new DorisColumnRowSerialize(
                        seaTunnelRowType, sinkConfig, sinkConfig.isEnableUpsertDelete());
        this.manager = new DorisSinkManager(sinkConfig, fieldNames);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String record = dorisColumnRowSerialize.serialize(element);
        manager.write(record);
    }

    @SneakyThrows
    @Override
    public Optional<Void> prepareCommit() {
        // Flush to storage before snapshot state is performed
        manager.flush();
        return super.prepareCommit();
    }

    @Override
    public void close() throws IOException {
        try {
            if (manager != null) {
                manager.close();
            }
        } catch (IOException e) {
            throw new DorisConnectorException(
                    CommonErrorCode.WRITER_OPERATION_FAILED, "Close doris manager failed.", e);
        }
    }
}
