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
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.StringUtils;

/**
 * A query to be executed based on a repository method, it's annotated SQL query and the arguments provided to the
 * method.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @since 1.0
 */
class JdbcRepositoryQuery implements RepositoryQuery {

	private static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method parameters. Use @Param for query method parameters, or when on Java 8+ use the javac flag -parameters.";

	private final JdbcQueryMethod queryMethod;
	private final JdbcMappingContext context;
	private final RowMapper<?> rowMapper;

	JdbcRepositoryQuery(JdbcQueryMethod queryMethod, JdbcMappingContext context, RowMapper defaultRowMapper) {

		this.queryMethod = queryMethod;
		this.context = context;
		this.rowMapper = createRowMapper(queryMethod, defaultRowMapper);
	}

	private static RowMapper<?> createRowMapper(JdbcQueryMethod queryMethod, RowMapper defaultRowMapper) {

		Class<?> rowMapperClass = queryMethod.getRowMapperClass();

		return rowMapperClass == null || rowMapperClass == RowMapper.class ? defaultRowMapper
				: (RowMapper<?>) BeanUtils.instantiateClass(rowMapperClass);
	}

	@Override
	public Object execute(Object[] objects) {

		String query = determineQuery();

		MapSqlParameterSource parameters = bindParameters(objects);

		if (queryMethod.isModifyingQuery()) {

			int updatedCount = context.getTemplate().update(query, parameters);
			Class<?> returnedObjectType = queryMethod.getReturnedObjectType();
			return (returnedObjectType == boolean.class || returnedObjectType == Boolean.class) ? updatedCount != 0
					: updatedCount;
		}

		if (queryMethod.isCollectionQuery() || queryMethod.isStreamQuery()) {
			return context.getTemplate().query(query, parameters, rowMapper);
		}

		try {
			return context.getTemplate().queryForObject(query, parameters, rowMapper);
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

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
}
