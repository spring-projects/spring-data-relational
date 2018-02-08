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
import java.util.Collections;

import org.springframework.core.convert.ConversionService;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.CustomConversions.StoreConversions;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.core.EntityRowMapper;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;

/**
 * {@link QueryLookupStrategy} for JDBC repositories. Currently only supports annotated queries.
 *
 * @author Jens Schauder
 * @author Kazuki Shimizu
 */
class JdbcQueryLookupStrategy implements QueryLookupStrategy {
	private static final SimpleTypeHolder SIMPLE_TYPE_HOLDER = new CustomConversions(StoreConversions.NONE,
			Collections.emptyList()).getSimpleTypeHolder();
	private final JdbcMappingContext context;
	private final DataAccessStrategy accessStrategy;
	private final ConversionService conversionService;

	JdbcQueryLookupStrategy(EvaluationContextProvider evaluationContextProvider, JdbcMappingContext context,
			DataAccessStrategy accessStrategy) {

		this.context = context;
		this.accessStrategy = accessStrategy;
		this.conversionService = context.getConversions();
	}

	@Override
	public RepositoryQuery resolveQuery(Method method, RepositoryMetadata repositoryMetadata,
			ProjectionFactory projectionFactory, NamedQueries namedQueries) {

		JdbcQueryMethod queryMethod = new JdbcQueryMethod(method, repositoryMetadata, projectionFactory);
		Class<?> returnedObjectType = queryMethod.getReturnedObjectType();
		RowMapper<?> rowMapper = SIMPLE_TYPE_HOLDER.isSimpleType(returnedObjectType)
				? new CustomSingleColumnRowMapper<>(returnedObjectType)
				: new EntityRowMapper<>(context.getRequiredPersistentEntity(returnedObjectType), conversionService,
						context, accessStrategy);
		return new JdbcRepositoryQuery(queryMethod, context, rowMapper);
	}
	
	private class CustomSingleColumnRowMapper<T> extends SingleColumnRowMapper<T> {

		private CustomSingleColumnRowMapper(Class<T> requiredType) {
			super(requiredType);
		}

		@Override
		protected Object convertValueToRequiredType(Object value, Class<?> requiredType) {
			return conversionService.canConvert(value.getClass(), requiredType)
					? conversionService.convert(value, requiredType)
					: super.convertValueToRequiredType(value, requiredType);
		}

	}

}
