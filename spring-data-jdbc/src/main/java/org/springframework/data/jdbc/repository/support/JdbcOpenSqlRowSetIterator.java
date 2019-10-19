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

import java.sql.SQLException;
import java.util.Iterator;

import org.springframework.jdbc.InvalidResultSetAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

/**
 * An iterator implementation that wraps a {@link JdbcOpenSqlRowSet} and applies
 * an {@link RowMapper} or {@link ResultSetExtractor} to each row.
 *
 * @author detinho
 */
class JdbcOpenSqlRowSetIterator<T> implements Iterator<T> {
    private final JdbcOpenSqlRowSet openCursorSqlRowSet;
    private final RowMapper<T> rowMapper;
    private final ResultSetExtractor<T> resultSetExtractor;
    private int rowIndex = 1;

    public JdbcOpenSqlRowSetIterator(JdbcOpenSqlRowSet openCursorSqlRowSet,
                                     RowMapper<T> rowMapper, ResultSetExtractor<T> resultSetExtractor) {
        this.openCursorSqlRowSet = openCursorSqlRowSet;
        this.rowMapper = rowMapper;
        this.resultSetExtractor = resultSetExtractor;
    }

    @Override
    public boolean hasNext() {
            return openCursorSqlRowSet.next();
    }

    @Override
    public T next() {
        try {
            if (rowMapper != null) {
                return rowMapper.mapRow(openCursorSqlRowSet.getResultSet(), rowIndex++);
            } else {
                return resultSetExtractor.extractData(openCursorSqlRowSet.getResultSet());
            }
        } catch (SQLException e) {
            throw new InvalidResultSetAccessException(e);
        }
    }
}
