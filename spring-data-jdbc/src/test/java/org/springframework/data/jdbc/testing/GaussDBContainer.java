/*
 * Copyright 2019-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.testing;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Testcontainers implementation for GaussDB.
 *
 * Exposed ports: 8000
 */
public class GaussDBContainer<SELF extends GaussDBContainer<SELF>> extends JdbcDatabaseContainer<SELF> {

    public static final String NAME = "gaussdb";

    public static final String IMAGE = "opengauss/opengauss";

    public static final String DEFAULT_TAG = "latest";

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("opengauss/opengauss")
        .asCompatibleSubstituteFor("gaussdb");

    public static final Integer GaussDB_PORT = 8000;

    public static final String DEFAULT_USER_NAME = "r2dbc_test";

    public static final String DEFAULT_PASSWORD = "R2dbc_test@12";

    private String databaseName = "postgres";

    private String username = DEFAULT_USER_NAME;

    private String password = DEFAULT_PASSWORD;

    public GaussDBContainer() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    public GaussDBContainer(final String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    public GaussDBContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        setWaitStrategy(new WaitStrategy() {
            @Override
            public void waitUntilReady(WaitStrategyTarget waitStrategyTarget) {
                Wait.forListeningPort().waitUntilReady(waitStrategyTarget);
                try {
                    // Open Gauss will set up users and password when ports are ready.
                    Wait.forLogMessage(".*gs_ctl stopped.*", 1).waitUntilReady(waitStrategyTarget);
                    // Not enough and no idea
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public WaitStrategy withStartupTimeout(Duration duration) {
                return Wait.forListeningPort().withStartupTimeout(duration);
            }
        });
    }

    @Override
    protected void configure() {
        // Disable Postgres driver use of java.util.logging to reduce noise at startup time
        withUrlParam("loggerLevel", "OFF");
        withDatabaseName(databaseName);
        addExposedPorts(GaussDB_PORT);
        addFixedExposedPort(GaussDB_PORT, GaussDB_PORT);
        addEnv("GS_PORT", String.valueOf(GaussDB_PORT));
        addEnv("GS_USERNAME", username);
        addEnv("GS_PASSWORD", password);
    }

    @Override
    public String getDriverClassName() {
        return "com.huawei.gaussdb.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return (
            "jdbc:gaussdb://" +
                getHost() +
                ":" +
                getMappedPort(GaussDB_PORT) +
                "/" +
                databaseName +
                additionalUrlParams
        );
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getTestQueryString() {
        return "SELECT 1";
    }

    @Override
    public SELF withDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
        return self();
    }

    @Override
    public SELF withUsername(final String username) {
        this.username = username;
        return self();
    }

    @Override
    public SELF withPassword(final String password) {
        this.password = password;
        return self();
    }

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }
}