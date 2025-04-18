/*
 * Copyright 2017-2025 the original author or authors.
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

import com.huawei.gaussdb.jdbc.ds.PGSimpleDataSource;
import com.huawei.gaussdb.jdbc.util.PSQLException;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;

/**
 * {@link DataSource} setup for GaussDB. Starts a docker container with a GaussDB database.
 * <p>
 * Notes: this file is token from PostgresDataSourceConfiguration and add specific changes for GaussDB
 *
 * @author liubao
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnDatabase(DatabaseType.GAUSSDB)
public class GaussDBDataSourceConfiguration extends DataSourceConfiguration {

    private static GaussDBContainer<?> GAUSSDB_CONTAINER;

    public GaussDBDataSourceConfiguration(TestClass testClass, Environment environment) {
        super(testClass, environment);
    }

    @Override
    protected DataSource createDataSource() {

        if (GAUSSDB_CONTAINER == null) {

            GaussDBContainer<?> container = new GaussDBContainer<>();
            container.start();

            GAUSSDB_CONTAINER = container;
        }

        try {
            PGSimpleDataSource dataSource = new PGSimpleDataSource();
            dataSource.setUrl(GAUSSDB_CONTAINER.getJdbcUrl());
            dataSource.setUser(GAUSSDB_CONTAINER.getUsername());
            dataSource.setPassword(GAUSSDB_CONTAINER.getPassword());

            return dataSource;
        } catch (PSQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void customizePopulator(ResourceDatabasePopulator populator) {
        populator.setIgnoreFailedDrops(true);
    }

}
