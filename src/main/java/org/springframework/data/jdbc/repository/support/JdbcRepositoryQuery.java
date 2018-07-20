/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.jdbc.repository.support;

import org.springframework.beans.BeanUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A query to be executed based on a repository method, it's annotated SQL query and the arguments provided to the
 * method.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 */
class JdbcRepositoryQuery implements RepositoryQuery {

	private static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method parameters. Use @Param for query method parameters, or when on Java 8+ use the javac flag -parameters.";

	private final JdbcQueryMethod queryMethod;
	private final NamedParameterJdbcOperations operations;
	private final RowMapper<?> rowMapper;

	/**
	 * Creates a new {@link JdbcRepositoryQuery} for the given {@link JdbcQueryMethod}, {@link RelationalMappingContext} and
	 * {@link RowMapper}.
	 * 
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param defaultRowMapper can be {@literal null} (only in case of a modifying query).
	 */
	JdbcRepositoryQuery(JdbcQueryMethod queryMethod, NamedParameterJdbcOperations operations,
			@Nullable RowMapper<?> defaultRowMapper) {

		Assert.notNull(queryMethod, "Query method must not be null!");
		Assert.notNull(operations, "NamedParameterJdbcOperations must not be null!");

		if (!queryMethod.isModifyingQuery()) {
			Assert.notNull(defaultRowMapper, "RowMapper must not be null!");
		}

		this.queryMethod = queryMethod;
		this.operations = operations;
		this.rowMapper = createRowMapper(queryMethod, defaultRowMapper);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	@Override
	public Object execute(Object[] objects) {

		String query = determineQuery();
		MapSqlParameterSource parameters = bindParameters(objects);

		if (queryMethod.isModifyingQuery()) {

			int updatedCount = operations.update(query, parameters);
			Class<?> returnedObjectType = queryMethod.getReturnedObjectType();
			return (returnedObjectType == boolean.class || returnedObjectType == Boolean.class) ? updatedCount != 0
					: updatedCount;
		}

		if (queryMethod.isCollectionQuery() || queryMethod.isStreamQuery()) {
			return operations.query(query, parameters, rowMapper);
		}

		try {
			return operations.queryForObject(query, parameters, rowMapper);
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	@Override
	public JdbcQueryMethod getQueryMethod() {
		return queryMethod;
	}

	private String determineQuery() {

		String query = queryMethod.getAnnotatedQuery();

		if (StringUtils.isEmpty(query)) {
			throw new IllegalStateException(String.format("No query specified on %s", queryMethod.getName()));
		}

		return query;
	}

	private MapSqlParameterSource bindParameters(Object[] objects) {

		MapSqlParameterSource parameters = new MapSqlParameterSource();

		queryMethod.getParameters().getBindableParameters().forEach(p -> {

			String parameterName = p.getName().orElseThrow(() -> new IllegalStateException(PARAMETER_NEEDS_TO_BE_NAMED));
			parameters.addValue(parameterName, objects[p.getIndex()]);
		});

		return parameters;
	}

	@Nullable
	private static RowMapper<?> createRowMapper(JdbcQueryMethod queryMethod, @Nullable RowMapper<?> defaultRowMapper) {

		Class<?> rowMapperClass = queryMethod.getRowMapperClass();

		return rowMapperClass == null || rowMapperClass == RowMapper.class //
				? defaultRowMapper //
				: (RowMapper<?>) BeanUtils.instantiateClass(rowMapperClass);
	}
}
