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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject;

import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.data.ClickHouseBitmap;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@Slf4j
public class StringInjectFunction implements ClickhouseFieldInjectFunction {

    private String fieldType;

    @Override
    public void injectFields(PreparedStatement statement, int index, Object value) throws SQLException {

        ObjectMapper mapper = new ObjectMapper();
        log.info("clickhouse-sink-inject index:{},value:{}", index, value);
        try {
            if ("AggregateFunction(groupBitmap, UInt32)".equals(fieldType)) {
                statement.setObject(index, mapper.readValue(value.toString(), int[].class));
            } else {
                statement.setString(index, value.toString());
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }

    @Override
    public boolean isCurrentFieldType(String fieldType) {
        this.fieldType = fieldType;

        return "String".equals(fieldType)
                || "Int128".equals(fieldType)
                || "UInt128".equals(fieldType)
                || "Int256".equals(fieldType)
                || "UInt256".equals(fieldType)
                || "AggregateFunction(groupBitmap, UInt32)".equals(fieldType);

    }


}
