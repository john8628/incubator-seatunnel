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

package org.apache.seatunnel.e2e.connector.doris;

import static org.awaitility.Awaitility.given;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class DorisCDCIT extends TestSuiteBase implements TestResource {
    private static final String DOCKER_IMAGE = "taozex/doris:tagname";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String HOST = "doris_e2e";
    private static final int DOCKER_PORT = 9030;
    private static final int PORT = 8961;
    private static final String URL = "jdbc:mysql://%s:" + PORT;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String DATABASE = "test";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String DRIVER_JAR = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";

    private static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS " + DATABASE;

    private static final String DDL_SINK = "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + SINK_TABLE + " (\n" +
            "pk_id  BIGINT,\n" +
            "name    VARCHAR(11),\n" +
            "score   INT \n" +
            ")ENGINE=OLAP\n" +
            "UNIQUE KEY(`pk_id`)\n" +
            "DISTRIBUTED BY HASH(`pk_id`) BUCKETS 1\n" +
            "PROPERTIES (\n" +
            "\"replication_allocation\" = \"tag.location.default: 1\"" +
            ")";

    private Connection jdbcConnection;
    private GenericContainer<?> dorisServer;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory = container -> {
        Container.ExecResult extraCommands = container.execInContainer("bash", "-c", "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O " + DRIVER_JAR);
        Assertions.assertEquals(0, extraCommands.getExitCode());
    };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        dorisServer = new GenericContainer<>(DOCKER_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases(HOST)
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        dorisServer.setPortBindings(Lists.newArrayList(
            String.format("%s:%s", PORT, DOCKER_PORT)));
        Startables.deepStart(Stream.of(dorisServer)).join();
        log.info("Doris container started");

        // wait for doris fully start
        given().ignoreExceptions()
            .await()
            .atMost(1000, TimeUnit.SECONDS)
            .untilAsserted(this::initializeJdbcConnection);
        initializeJdbcTable();

    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (dorisServer != null) {
            dorisServer.close();
        }
    }

    @TestTemplate
    public void testDorisSink(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/write-cdc-changelog-to-doris.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        try  {
            assertHasData(SINK_TABLE);
            String sinkSql = String.format("select * from %s.%s", DATABASE, SINK_TABLE);
            Statement sinkStatement = jdbcConnection.createStatement();
            ResultSet sinkResultSet = sinkStatement.executeQuery(sinkSql);
            Set<List<Object>> actual = new HashSet<>();
            while (sinkResultSet.next()) {
                List<Object> row = Arrays.asList(
                     sinkResultSet.getLong("pk_id"),
                     sinkResultSet.getString("name"),
                     sinkResultSet.getInt("score"));
                actual.add(row);
            }
            log.info(String.valueOf(actual.size()));
            log.info(String.valueOf(actual));
            Set<List<Object>> expected = Stream.<List<Object>>of(
                Arrays.asList(1L, "A_1", 100),
                Arrays.asList(3L, "C", 100))
                .collect(Collectors.toSet());
            Assertions.assertIterableEquals(expected, actual);
            clearSinkTable();
        } catch (Exception e) {
            throw new RuntimeException("Get doris connection error", e);
        }
    }

    private void initializeJdbcConnection() throws SQLException, ClassNotFoundException, MalformedURLException, InstantiationException, IllegalAccessException, InterruptedException {
        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{new URL(DRIVER_JAR)}, DorisCDCIT.class.getClassLoader());
        Thread.currentThread().setContextClassLoader(urlClassLoader);
        Driver driver = (Driver) urlClassLoader.loadClass(DRIVER_CLASS).newInstance();
        Properties props = new Properties();
        props.put("user", USERNAME);
        props.put("password", PASSWORD);
        jdbcConnection =  driver.connect(String.format(URL, dorisServer.getHost()), props);
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(CREATE_DATABASE);
            Thread.sleep(10000);
        }
        log.info("create statement");
    }

    private void initializeJdbcTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            Thread.sleep(10000);
            // create databases
            statement.execute(CREATE_DATABASE);
            // create sink table
            log.info(DDL_SINK);
            statement.execute(DDL_SINK);
        } catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Initializing table failed!", e);
        }
    }

    private void assertHasData(String table) {
        try (Statement statement = jdbcConnection.createStatement()) {
            String sql = String.format("select * from %s.%s limit 1", DATABASE, table);
            ResultSet source = statement.executeQuery(sql);
            Assertions.assertTrue(source.next());
        } catch (Exception e) {
            throw new RuntimeException("Test doris server image error", e);
        }
    }

    private void clearSinkTable() {
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(String.format("TRUNCATE TABLE %s.%s", DATABASE, SINK_TABLE));
        } catch (SQLException e) {
            throw new RuntimeException("Test doris server image error", e);
        }
    }
}
