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

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.lang.reflect.Method;
import java.util.Optional;

import org.springframework.data.jdbc.core.function.DatabaseClient;
import org.springframework.data.jdbc.core.function.MappingR2dbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntity;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentProperty;
import org.springframework.data.jdbc.repository.query.JdbcEntityInformation;
import org.springframework.data.jdbc.repository.query.R2dbcQueryMethod;
import org.springframework.data.jdbc.repository.query.StringBasedR2dbcQuery;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.ReactiveRepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Factory to create {@link org.springframework.data.jdbc.repository.R2dbcRepository} instances.
 *
 * @author Mark Paluch
 */
public class R2dbcRepositoryFactory extends ReactiveRepositoryFactorySupport {

	private static final SpelExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();

	private final DatabaseClient databaseClient;
	private final MappingContext<? extends JdbcPersistentEntity<?>, JdbcPersistentProperty> mappingContext;
	private final MappingR2dbcConverter converter;

	/**
	 * Creates a new {@link R2dbcRepositoryFactory} given {@link DatabaseClient} and {@link MappingContext}.
	 *
	 * @param databaseClient must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public R2dbcRepositoryFactory(DatabaseClient databaseClient,
			MappingContext<? extends JdbcPersistentEntity<?>, JdbcPersistentProperty> mappingContext) {

		Assert.notNull(databaseClient, "DatabaseClient must not be null!");
		Assert.notNull(mappingContext, "MappingContext must not be null!");

		this.databaseClient = databaseClient;
		this.mappingContext = mappingContext;
		this.converter = new MappingR2dbcConverter(mappingContext);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getRepositoryBaseClass(org.springframework.data.repository.core.RepositoryMetadata)
	 */
	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleR2dbcRepository.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getTargetRepository(org.springframework.data.repository.core.RepositoryInformation)
	 */
	@Override
	protected Object getTargetRepository(RepositoryInformation information) {

		JdbcEntityInformation<?, ?> entityInformation = getEntityInformation(information.getDomainType(), information);

		return getTargetRepositoryViaReflection(information, entityInformation, databaseClient, converter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getQueryLookupStrategy(org.springframework.data.repository.query.QueryLookupStrategy.Key, org.springframework.data.repository.query.EvaluationContextProvider)
	 */
	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable Key key,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {
		return Optional.of(new R2dbcQueryLookupStrategy(databaseClient, evaluationContextProvider, converter));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getEntityInformation(java.lang.Class)
	 */
	public <T, ID> JdbcEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
		return getEntityInformation(domainClass, null);
	}

	@SuppressWarnings("unchecked")
	private <T, ID> JdbcEntityInformation<T, ID> getEntityInformation(Class<T> domainClass,
			@Nullable RepositoryInformation information) {

		JdbcPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(domainClass);

		return new MappingJdbcEntityInformation<>((JdbcPersistentEntity<T>) entity);
	}

	/**
	 * {@link QueryLookupStrategy} to create R2DBC queries..
	 *
	 * @author Mark Paluch
	 */
	@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
	private static class R2dbcQueryLookupStrategy implements QueryLookupStrategy {

		private final DatabaseClient databaseClient;
		private final QueryMethodEvaluationContextProvider evaluationContextProvider;
		private final MappingR2dbcConverter converter;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.QueryLookupStrategy#resolveQuery(java.lang.reflect.Method, org.springframework.data.repository.core.RepositoryMetadata, org.springframework.data.projection.ProjectionFactory, org.springframework.data.repository.core.NamedQueries)
		 */
		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			R2dbcQueryMethod queryMethod = new R2dbcQueryMethod(method, metadata, factory, converter.getMappingContext());
			String namedQueryName = queryMethod.getNamedQueryName();

			if (namedQueries.hasQuery(namedQueryName)) {
				String namedQuery = namedQueries.getQuery(namedQueryName);
				return new StringBasedR2dbcQuery(namedQuery, queryMethod, databaseClient, converter, EXPRESSION_PARSER,
						evaluationContextProvider);
			} else if (queryMethod.hasAnnotatedQuery()) {
				return new StringBasedR2dbcQuery(queryMethod, databaseClient, converter, EXPRESSION_PARSER,
						evaluationContextProvider);
			}

			throw new UnsupportedOperationException("Query derivation not yet supported!");

		}
	}
}
