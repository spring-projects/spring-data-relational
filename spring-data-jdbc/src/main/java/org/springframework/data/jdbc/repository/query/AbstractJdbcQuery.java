/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.jdbc.repository.query;

import java.util.List;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Base class for queries based on a repository method. It holds the infrastructure for executing a query and knows how
 * to execute a query based on the return type of the method. How to construct the query is left to subclasses.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Maciej Walkowiak
 * @author Mark Paluch
 * @since 2.0
 */
public abstract class AbstractJdbcQuery implements RepositoryQuery {

	private final JdbcQueryMethod queryMethod;
	private final NamedParameterJdbcOperations operations;

	/**
	 * Creates a new {@link AbstractJdbcQuery} for the given {@link JdbcQueryMethod}, {@link NamedParameterJdbcOperations}
	 * and {@link RowMapper}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param defaultRowMapper can be {@literal null} (only in case of a modifying query).
	 */
	AbstractJdbcQuery(JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations,
			@Nullable RowMapper<?> defaultRowMapper) {

		Assert.notNull(queryMethod, "Query method must not be null!");
		Assert.notNull(operations, "NamedParameterJdbcOperations must not be null!");

		if (!queryMethod.isModifyingQuery()) {
			Assert.notNull(defaultRowMapper, "Mapper must not be null!");
		}

		this.queryMethod = queryMethod;
		this.operations = operations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	@Override
	public JdbcQueryMethod getQueryMethod() {
		return queryMethod;
	}

	/**
	 * Creates a {@link JdbcQueryExecution} given {@link JdbcQueryMethod}, {@link ResultSetExtractor} an
	 * {@link RowMapper}. Prefers the given {@link ResultSetExtractor} over {@link RowMapper}.
	 * 
	 * @param queryMethod must not be {@literal null}.
	 * @param extractor must not be {@literal null}.
	 * @param rowMapper must not be {@literal null}.
	 * @return
	 */
	protected JdbcQueryExecution<?> getQueryExecution(JdbcQueryMethod queryMethod,
			@Nullable ResultSetExtractor<Object> extractor, RowMapper<Object> rowMapper) {

		if (queryMethod.isModifyingQuery()) {
			return createModifyingQueryExecutor();
		}

		if (queryMethod.isCollectionQuery() || queryMethod.isStreamQuery()) {
			return extractor != null ? getQueryExecution(extractor) : collectionQuery(rowMapper);
		}

		return extractor != null ? getQueryExecution(extractor) : singleObjectQuery(rowMapper);
	}

	private JdbcQueryExecution<Object> createModifyingQueryExecutor() {

		return (query, parameters) -> {

			int updatedCount = operations.update(query, parameters);
			Class<?> returnedObjectType = queryMethod.getReturnedObjectType();

			return (returnedObjectType == boolean.class || returnedObjectType == Boolean.class) ? updatedCount != 0
					: updatedCount;
		};
	}

	private JdbcQueryExecution<Object> singleObjectQuery(RowMapper<?> rowMapper) {

		return (query, parameters) -> {
			try {
				return operations.queryForObject(query, parameters, rowMapper);
			} catch (EmptyResultDataAccessException e) {
				return null;
			}
		};
	}

	private <T> JdbcQueryExecution<List<T>> collectionQuery(RowMapper<T> rowMapper) {
		return getQueryExecution(new RowMapperResultSetExtractor<>(rowMapper));
	}

	private <T> JdbcQueryExecution<T> getQueryExecution(ResultSetExtractor<T> resultSetExtractor) {
		return (query, parameters) -> operations.query(query, parameters, resultSetExtractor);
	}

}
