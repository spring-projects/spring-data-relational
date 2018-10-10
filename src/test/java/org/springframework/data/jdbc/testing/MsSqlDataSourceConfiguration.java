/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.testing;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.testcontainers.containers.MSSQLServerContainer;

import javax.sql.DataSource;


/**
 * {@link DataSource} setup for PostgreSQL.
 * <p>
 * Configuration for a MSSQL Datasource.
 * As there is no testcontainer image to use we have to do it the following way:
 *
 * @author Thomas Lang
 * @see <a href="https://github.com/testcontainers/testcontainers-java/tree/master/modules/mssqlserver"></a>
 */
@Configuration
@Profile({"mssql"})
public class MsSqlDataSourceConfiguration extends DataSourceConfiguration {

    private static final MSSQLServerContainer mssqlserver = new MSSQLServerContainer();

    static {
        mssqlserver.start();
    }


    /*
     * (non-Javadoc)
     * @see org.springframework.data.jdbc.testing.DataSourceConfiguration#createDataSource()
     */
    @Override
    protected DataSource createDataSource() {
        SQLServerDataSource sqlServerDataSource = new SQLServerDataSource();
        sqlServerDataSource.setURL(mssqlserver.getJdbcUrl());
        sqlServerDataSource.setUser(mssqlserver.getUsername());
        sqlServerDataSource.setPassword(mssqlserver.getPassword());
        return sqlServerDataSource;
    }
}
