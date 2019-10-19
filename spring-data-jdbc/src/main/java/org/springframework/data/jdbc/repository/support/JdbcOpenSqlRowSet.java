/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.jdbc.repository.support;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.springframework.jdbc.InvalidResultSetAccessException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.rowset.ResultSetWrappingSqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSet;

/**
 * An implementation of Spring's {@link SqlRowSet} interface, wrapping a
 * <b>connected</b> {@link ResultSet}. This implementation also keeps track of
 * the {@link DataSource} that originated the {@link ResultSet} and releases
 * the resources when {@link JdbcOpenSqlRowSet#close()} is called.
 *
 * @author detinho
 */
class JdbcOpenSqlRowSet extends ResultSetWrappingSqlRowSet implements AutoCloseable {
    private final DataSource dataSource;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Create a new JdbcOpenSqlRowSet for the given ResultSet.
     *
     * @param resultSet  a <b>connected</b> ResultSet to wrap. The client code is responsible
     *                   for closing the resources
     * @param dataSource the dataSource that originated the ResultSet connection
     * @throws InvalidResultSetAccessException if extracting
     *                                         the ResultSetMetaData failed
     * @see java.sql.ResultSet#getMetaData
     * @see JdbcOpenSqlRowSet
     */
    public JdbcOpenSqlRowSet(DataSource dataSource, ResultSet resultSet) {
        super(resultSet);
        this.dataSource = dataSource;
    }

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            try {
                final Statement statement = getResultSet().getStatement();
                final Connection connection = statement.getConnection();

                JdbcUtils.closeResultSet(getResultSet());
                JdbcUtils.closeStatement(statement);
                DataSourceUtils.releaseConnection(connection, dataSource);
            } catch (SQLException e) {
                throw new InvalidResultSetAccessException(e);
            }
        }
    }
}