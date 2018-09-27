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
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;

/**
 * {@link DataSource} setup for PostgreSQL.
 * <p>
 * Configuration for a MSSQL Datasource.
 * As there is no testcontainer image to use we have to do it the following way:
 *
 * @author Thomas Lang
 * @see <a href="https://docs.microsoft.com/de-de/sql/linux/quickstart-install-connect-docker?view=sql-server-2017"></a>
 * <p>
 * (Docker installed and running is assumed)
 * 1. docker pull mcr.microsoft.com/mssql/server:2017-latest
 * 2. docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=<YourStrong!Passw0rd>" -p 1433:1433 --name sql1 -d mcr.microsoft.com/mssql/server:2017-latest
 * 3. add mssql jdbc driver maven dependency from here https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc/7.1.0.jre10-preview
 */
@Configuration
@Profile("mssql")
public class MsSqlDataSourceConfiguration extends DataSourceConfiguration {

    /*
     * (non-Javadoc)
     * @see org.springframework.data.jdbc.testing.DataSourceConfiguration#createDataSource()
     */
    @Override
    protected DataSource createDataSource() {
        SQLServerDataSource sqlServerDataSource = new SQLServerDataSource();
        sqlServerDataSource.setURL("");
        sqlServerDataSource.setUser("");
        sqlServerDataSource.setPassword("");
        return sqlServerDataSource;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.jdbc.testing.DataSourceFactoryBean#customizePopulator(org.springframework.jdbc.datasource.init.ResourceDatabasePopulator)
     */
    @Override
    protected void customizePopulator(ResourceDatabasePopulator populator) {
        populator.setIgnoreFailedDrops(true);
    }
}
