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

package org.apache.seatunnel.connectors.doris.serialize;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.doris.config.SinkConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DorisColumnRowSerialize implements SeaTunnelRowSerializer {
    /** Ø RowType to generate the runtime converter. */
    private final SeaTunnelRowType seaTunnelRowType;

    private final SinkConfig sinkConfig;

    private DorisBaseSerializer dorisBaseSerializer;

    private final boolean enableDelete;

    public DorisColumnRowSerialize(
            SeaTunnelRowType rowType, SinkConfig sinkConfig, boolean enableDelete) {
        this.seaTunnelRowType = rowType;
        this.sinkConfig = sinkConfig;
        this.enableDelete = enableDelete;
        this.dorisBaseSerializer = new DorisBaseSerializer();
    }

    @Override
    public String serialize(SeaTunnelRow seaTunnelRow) throws IOException {
        if (SinkConfig.StreamLoadFormat.CSV.equals(sinkConfig.getLoadFormat())) {
            return buildCSVString(seaTunnelRow);
        }
        if (SinkConfig.StreamLoadFormat.JSON.equals(sinkConfig.getLoadFormat())) {
            return buildJsonString(seaTunnelRow);
        }
        return null;
    }

    public String buildJsonString(SeaTunnelRow row) throws IOException {
        Map<String, Object> rowMap = new HashMap<>(row.getFields().length);
        for (int i = 0; i < row.getFields().length; i++) {
            Object value =
                    dorisBaseSerializer.convert(seaTunnelRowType.getFieldType(i), row.getField(i));
            rowMap.put(seaTunnelRowType.getFieldName(i), value);
        }
        if (enableDelete) {
            rowMap.put("__DORIS_DELETE_SIGN__", parseDeleteSign(row.getRowKind()));
        }
        return JsonUtils.toJsonString(rowMap);
    }

    public String buildCSVString(SeaTunnelRow row) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.getFields().length; i++) {
            Object value =
                    dorisBaseSerializer.convert(seaTunnelRowType.getFieldType(i), row.getField(i));
            sb.append(null == value ? "\\N" : value);
        }
        if (enableDelete) {
            sb.append(parseDeleteSign(row.getRowKind()));
        }
        return sb.toString();
    }

    public String parseDeleteSign(RowKind rowKind) {
        if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_AFTER.equals(rowKind)) {
            return "0";
        } else if (RowKind.DELETE.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
            return "1";
        } else {
            throw new IllegalArgumentException("Unrecognized row kind:" + rowKind.toString());
        }
    }
}
