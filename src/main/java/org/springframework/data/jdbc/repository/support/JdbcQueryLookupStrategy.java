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

import java.lang.reflect.Method;

import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.core.EntityRowMapper;
import org.springframework.data.jdbc.repository.RowMapperMap;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.util.Assert;

/**
 * {@link QueryLookupStrategy} for JDBC repositories. Currently only supports annotated queries.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 * @author Oliver Gierke
 * @author Mark Paluch
 */
class JdbcQueryLookupStrategy implements QueryLookupStrategy {

	private final RelationalMappingContext context;
	private final RelationalConverter converter;
	private final DataAccessStrategy accessStrategy;
	private final RowMapperMap rowMapperMap;
	private final NamedParameterJdbcOperations operations;

	/**
	 * Creates a new {@link JdbcQueryLookupStrategy} for the given {@link RelationalMappingContext},
	 * {@link DataAccessStrategy} and {@link RowMapperMap}.
	 *
	 * @param context must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param accessStrategy must not be {@literal null}.
	 * @param rowMapperMap must not be {@literal null}.
	 */
	JdbcQueryLookupStrategy(RelationalMappingContext context, RelationalConverter converter,
			DataAccessStrategy accessStrategy, RowMapperMap rowMapperMap, NamedParameterJdbcOperations operations) {

		Assert.notNull(context, "RelationalMappingContext must not be null!");
		Assert.notNull(converter, "RelationalConverter must not be null!");
		Assert.notNull(accessStrategy, "DataAccessStrategy must not be null!");
		Assert.notNull(rowMapperMap, "RowMapperMap must not be null!");

		this.context = context;
		this.converter = converter;
		this.accessStrategy = accessStrategy;
		this.rowMapperMap = rowMapperMap;
		this.operations = operations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryLookupStrategy#resolveQuery(java.lang.reflect.Method, org.springframework.data.repository.core.RepositoryMetadata, org.springframework.data.projection.ProjectionFactory, org.springframework.data.repository.core.NamedQueries)
	 */
	@Override
	public RepositoryQuery resolveQuery(Method method, RepositoryMetadata repositoryMetadata,
			ProjectionFactory projectionFactory, NamedQueries namedQueries) {

		JdbcQueryMethod queryMethod = new JdbcQueryMethod(method, repositoryMetadata, projectionFactory);

		RowMapper<?> rowMapper = queryMethod.isModifyingQuery() ? null : createRowMapper(queryMethod);

		return new JdbcRepositoryQuery(queryMethod, operations, rowMapper);
	}

	private RowMapper<?> createRowMapper(JdbcQueryMethod queryMethod) {

		Class<?> returnedObjectType = queryMethod.getReturnedObjectType();

		RelationalPersistentEntity<?> persistentEntity = context.getPersistentEntity(returnedObjectType);

		if (persistentEntity == null) {
			return SingleColumnRowMapper.newInstance(returnedObjectType, converter.getConversionService());
		}

		return determineDefaultRowMapper(queryMethod);
	}

	private RowMapper<?> determineDefaultRowMapper(JdbcQueryMethod queryMethod) {

		Class<?> domainType = queryMethod.getReturnedObjectType();

		RowMapper<?> typeMappedRowMapper = rowMapperMap.rowMapperFor(domainType);

		return typeMappedRowMapper == null //
				? new EntityRowMapper<>( //
						context.getRequiredPersistentEntity(domainType), //
						context, //
						converter, //
						accessStrategy) //
				: typeMappedRowMapper;
	}
}
