/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository.support;

import java.lang.reflect.Method;
import java.util.Optional;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.r2dbc.repository.query.PartTreeR2dbcQuery;
import org.springframework.data.r2dbc.repository.query.R2dbcQueryMethod;
import org.springframework.data.r2dbc.repository.query.StringBasedR2dbcQuery;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.relational.repository.support.MappingRelationalEntityInformation;
import org.springframework.data.relational.repository.support.RelationalQueryLookupStrategy;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.ReactiveRepositoryFactorySupport;
import org.springframework.data.repository.query.CachingValueExpressionDelegate;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.util.Assert;

/**
 * Factory to create {@link R2dbcRepository} instances.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Marcin Grzejszczak
 */
public class R2dbcRepositoryFactory extends ReactiveRepositoryFactorySupport {

	private final DatabaseClient databaseClient;
	private final ReactiveDataAccessStrategy dataAccessStrategy;
	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;
	private final R2dbcConverter converter;
	private final R2dbcEntityOperations operations;

	/**
	 * Creates a new {@link R2dbcRepositoryFactory} given {@link DatabaseClient} and {@link MappingContext}.
	 *
	 * @param databaseClient must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 */
	public R2dbcRepositoryFactory(DatabaseClient databaseClient, ReactiveDataAccessStrategy dataAccessStrategy) {

		Assert.notNull(databaseClient, "DatabaseClient must not be null");
		Assert.notNull(dataAccessStrategy, "ReactiveDataAccessStrategy must not be null");

		this.databaseClient = databaseClient;
		this.dataAccessStrategy = dataAccessStrategy;
		this.converter = dataAccessStrategy.getConverter();
		this.mappingContext = this.converter.getMappingContext();
		this.operations = new R2dbcEntityTemplate(this.databaseClient, this.dataAccessStrategy);
		setEvaluationContextProvider(ReactiveQueryMethodEvaluationContextProvider.DEFAULT);
	}

	/**
	 * Creates a new {@link R2dbcRepositoryFactory} given {@link R2dbcEntityOperations}.
	 *
	 * @param operations must not be {@literal null}.
	 * @since 1.1.3
	 */
	public R2dbcRepositoryFactory(R2dbcEntityOperations operations) {

		Assert.notNull(operations, "R2dbcEntityOperations must not be null");

		this.databaseClient = operations.getDatabaseClient();
		this.dataAccessStrategy = operations.getDataAccessStrategy();
		this.converter = dataAccessStrategy.getConverter();
		this.mappingContext = this.converter.getMappingContext();
		this.operations = operations;
		setEvaluationContextProvider(ReactiveQueryMethodEvaluationContextProvider.DEFAULT);
	}

	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleR2dbcRepository.class;
	}

	@Override
	protected Object getTargetRepository(RepositoryInformation information) {

		RelationalEntityInformation<?, ?> entityInformation = getEntityInformation(information.getDomainType(),
				information);

		return getTargetRepositoryViaReflection(information, entityInformation, operations, this.converter);
	}

	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable Key key,
			ValueExpressionDelegate valueExpressionDelegate) {
		return Optional.of(new R2dbcQueryLookupStrategy(operations, new CachingValueExpressionDelegate(valueExpressionDelegate), converter, dataAccessStrategy));
	}

	public <T, ID> RelationalEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
		return getEntityInformation(domainClass, null);
	}

	@SuppressWarnings("unchecked")
	private <T, ID> RelationalEntityInformation<T, ID> getEntityInformation(Class<T> domainClass,
			@Nullable RepositoryInformation information) {

		RelationalPersistentEntity<?> entity = this.mappingContext.getRequiredPersistentEntity(domainClass);

		return new MappingRelationalEntityInformation<>((RelationalPersistentEntity<T>) entity);
	}

	/**
	 * {@link QueryLookupStrategy} to create R2DBC queries.
	 *
	 * @author Mark Paluch
	 * @author Jens Schauder
	 */
	private static class R2dbcQueryLookupStrategy extends RelationalQueryLookupStrategy {

		private final R2dbcEntityOperations entityOperations;
		private final R2dbcConverter converter;
		private final ValueExpressionDelegate delegate;
		private final ReactiveDataAccessStrategy dataAccessStrategy;

		R2dbcQueryLookupStrategy(R2dbcEntityOperations entityOperations,
				ValueExpressionDelegate delegate, R2dbcConverter converter,
				ReactiveDataAccessStrategy dataAccessStrategy) {

			super(converter.getMappingContext(), dataAccessStrategy.getDialect());
			this.delegate = delegate;
			this.entityOperations = entityOperations;
			this.converter = converter;
			this.dataAccessStrategy = dataAccessStrategy;
		}

		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			R2dbcQueryMethod queryMethod = new R2dbcQueryMethod(method, metadata, factory, getMappingContext());
			String namedQueryName = queryMethod.getNamedQueryName();

			if (namedQueries.hasQuery(namedQueryName) || queryMethod.hasAnnotatedQuery()) {

				String query = namedQueries.hasQuery(namedQueryName) ? namedQueries.getQuery(namedQueryName)
						: queryMethod.getRequiredAnnotatedQuery();
				query = evaluateTableExpressions(metadata, query);

				return new StringBasedR2dbcQuery(query, queryMethod, this.entityOperations, this.converter, this.dataAccessStrategy, this.delegate);

			} else {
				return new PartTreeR2dbcQuery(queryMethod, this.entityOperations, this.converter, this.dataAccessStrategy);
			}
		}
	}
}
