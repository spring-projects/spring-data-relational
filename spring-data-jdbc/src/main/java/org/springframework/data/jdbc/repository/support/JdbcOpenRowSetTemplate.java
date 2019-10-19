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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.InvalidResultSetAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.util.Assert;

/**
 * An extended {@link NamedParameterJdbcTemplate} to return an {@link JdbcOpenSqlRowSet}.
 *
 * @author detinho
 */
class JdbcOpenRowSetTemplate extends NamedParameterJdbcTemplate {

    JdbcOpenRowSetTemplate(JdbcOperations classicJdbcTemplate) {
        super(classicJdbcTemplate);
    }

    JdbcOpenSqlRowSet queryForOpenCursorRowSet(String sql, SqlParameterSource paramSource, Integer fetchSize)  {
        Assert.state(this.getJdbcTemplate().getDataSource() != null, "No DataSource set");
        Assert.state(fetchSize != null, "No fetchSize set");

        Connection connection = DataSourceUtils.getConnection(this.getJdbcTemplate().getDataSource());
        PreparedStatementCreator preparedStatementCreator = this.getPreparedStatementCreator(sql, paramSource);
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = preparedStatementCreator.createPreparedStatement(connection);
            preparedStatement.setFetchSize(fetchSize);
            resultSet = preparedStatement.executeQuery();

            return new JdbcOpenSqlRowSet(this.getJdbcTemplate().getDataSource(), resultSet);
        } catch (SQLException e) {
            JdbcUtils.closeResultSet(resultSet);
            JdbcUtils.closeStatement(preparedStatement);
            DataSourceUtils.releaseConnection(connection, this.getJdbcTemplate().getDataSource());

            throw new InvalidResultSetAccessException(e);
        }
    }
}
