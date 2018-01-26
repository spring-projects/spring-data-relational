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

import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * A query to be executed based on a repository method, it's annotated SQL query and the arguments provided to the
 * method.
 *
 * @author Jens Schauder
 */
class JdbcRepositoryQuery implements RepositoryQuery {

	private static final String PARAMETER_NEEDS_TO_BE_NAMED = "For queries with named parameters you need to provide names for method parameters. Use @Param for query method parameters, or when on Java 8+ use the javac flag -parameters.";

	private final JdbcQueryMethod queryMethod;
	private final JdbcMappingContext context;
	private final RowMapper<?> rowMapper;

	JdbcRepositoryQuery(JdbcQueryMethod queryMethod, JdbcMappingContext context, RowMapper rowMapper) {

		this.queryMethod = queryMethod;
		this.context = context;
		this.rowMapper = rowMapper;
	}

	@Override
	public Object execute(Object[] objects) {

		String query = queryMethod.getAnnotatedQuery();

		MapSqlParameterSource parameters = new MapSqlParameterSource();
		queryMethod.getParameters().getBindableParameters().forEach(p -> {

			String parameterName = p.getName().orElseThrow(() -> new IllegalStateException(PARAMETER_NEEDS_TO_BE_NAMED));
			parameters.addValue(parameterName, objects[p.getIndex()]);
		});

		return context.getTemplate().query(query, parameters, rowMapper);
	}

	@Override
	public JdbcQueryMethod getQueryMethod() {
		return queryMethod;
	}
}
