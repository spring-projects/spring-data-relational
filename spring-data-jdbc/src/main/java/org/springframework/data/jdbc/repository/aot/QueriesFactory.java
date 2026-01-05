/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.jdbc.repository.aot;

import java.io.IOException;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.jspecify.annotations.Nullable;

import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.core.TypeInformation;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.Identifier;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.jdbc.core.convert.JdbcTypeFactory;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.jdbc.core.dialect.JdbcDialect;
import org.springframework.data.jdbc.core.mapping.JdbcValue;
import org.springframework.data.jdbc.repository.config.JdbcRepositoryConfigExtension;
import org.springframework.data.jdbc.repository.query.JdbcCountQueryCreator;
import org.springframework.data.jdbc.repository.query.JdbcParameters;
import org.springframework.data.jdbc.repository.query.JdbcQueryCreator;
import org.springframework.data.jdbc.repository.query.JdbcQueryMethod;
import org.springframework.data.jdbc.repository.query.ParameterBinding;
import org.springframework.data.jdbc.repository.query.ParametrizedQuery;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentPropertyPathAccessor;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.data.relational.repository.query.ParameterMetadataProvider;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalParameters;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.config.PropertiesBasedNamedQueriesFactoryBean;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.repository.query.ValueExpressionQueryRewriter;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.StringUtils;

/**
 * Factory for {@link AotQueries}.
 *
 * @author Mark Paluch
 * @since 4.0
 */
class QueriesFactory {

	private final JdbcConverter converter;
	private final JdbcDialect dialect;
	private final NamedQueries namedQueries;
	private final ValueExpressionDelegate delegate;

	public QueriesFactory(RepositoryConfigurationSource configurationSource, JdbcDialect dialect,
			RelationalMappingContext mappingContext, ClassLoader classLoader, ValueExpressionDelegate delegate) {

		this.converter = new MappingJdbcConverter(mappingContext, (identifier, path) -> List.of(),
				JdbcCustomConversions.of(dialect, List.of()), JdbcTypeFactory.unsupported());

		this.namedQueries = getNamedQueries(configurationSource, classLoader);
		this.dialect = dialect;
		this.delegate = delegate;
	}

	public NamedQueries getNamedQueries() {
		return namedQueries;
	}

	private NamedQueries getNamedQueries(@Nullable RepositoryConfigurationSource configSource, ClassLoader classLoader) {

		String location = configSource != null ? configSource.getNamedQueryLocation().orElse(null) : null;

		if (location == null) {
			location = new JdbcRepositoryConfigExtension().getDefaultNamedQueryLocation();
		}

		if (StringUtils.hasText(location)) {

			try {

				PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(classLoader);

				PropertiesBasedNamedQueriesFactoryBean factoryBean = new PropertiesBasedNamedQueriesFactoryBean();
				factoryBean.setLocations(resolver.getResources(location));
				factoryBean.afterPropertiesSet();
				return Objects.requireNonNull(factoryBean.getObject());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		return new PropertiesBasedNamedQueries(new Properties());
	}

	/**
	 * Creates the {@link AotQueries} used within a specific {@link JdbcQueryMethod}.
	 *
	 * @param repositoryInformation
	 * @param returnedType
	 * @param query
	 * @param queryMethod
	 * @return
	 */
	public AotQueries createQueries(RepositoryInformation repositoryInformation, ReturnedType returnedType,
			MergedAnnotation<Query> query, JdbcQueryMethod queryMethod) {

		if (query.isPresent() && StringUtils.hasText(query.getString("value"))) {
			return buildStringQuery(query.getString("value"), queryMethod);
		}

		String queryName = queryMethod.getNamedQueryName();
		if (hasNamedQuery(queryName)) {
			return buildNamedQuery(queryName, queryMethod);
		}

		return buildPartTreeQuery(repositoryInformation, returnedType, queryMethod);
	}

	private boolean hasNamedQuery(String queryName) {
		return namedQueries.hasQuery(queryName);
	}

	private AotQueries buildStringQuery(String queryString, JdbcQueryMethod queryMethod) {

		ValueExpressionQueryRewriter.ParsedQuery parsedQuery = parseQuery(queryString);

		List<ParameterBinding> bindings = getBindings(parsedQuery, queryMethod);
		StringAotQuery aotStringQuery = StringAotQuery.of(parsedQuery.getQueryString(), bindings);

		return AotQueries.create(aotStringQuery);
	}

	private AotQueries buildNamedQuery(String queryName, JdbcQueryMethod queryMethod) {

		String queryString = namedQueries.getQuery(queryName);
		ValueExpressionQueryRewriter.ParsedQuery parsedQuery = parseQuery(queryString);

		return AotQueries.create(StringAotQuery.named(queryName, queryString, getBindings(parsedQuery, queryMethod)));
	}

	private AotQueries buildPartTreeQuery(RepositoryInformation repositoryInformation, ReturnedType returnedType,
			JdbcQueryMethod queryMethod) {

		PartTree partTree = new PartTree(queryMethod.getName(), repositoryInformation.getDomainType());
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod);

		JdbcQueryCreator queryCreator = new JdbcQueryCreator(partTree, new AotPassThruJdbcConverter(converter), dialect,
				queryMethod, accessor,
				returnedType) {

			@Override
			protected ParameterMetadataProvider getParameterMetadataProvider(RelationalParameterAccessor accessor) {
				return new PlaceholderAccessor.CapturingParameterMetadataProvider(accessor);
			}
		};

		ParametrizedQuery query = queryCreator.createQuery(Sort.unsorted());
		DerivedAotQuery aotQuery = new DerivedAotQuery(query, partTree, partTree.isCountProjection());

		if (queryMethod.isPageQuery()) {

			JdbcQueryCreator countQueryCreator = new JdbcCountQueryCreator(partTree, converter, dialect, queryMethod,
					accessor, returnedType) {

				@Override
				protected ParameterMetadataProvider getParameterMetadataProvider(RelationalParameterAccessor accessor) {
					return PlaceholderAccessor.metadata(accessor);
				}
			};

			ParametrizedQuery countQuery = countQueryCreator.createQuery(Sort.unsorted());
			DerivedAotQuery aotCountQuery = new DerivedAotQuery(countQuery, partTree, true);

			return AotQueries.create(aotQuery, aotCountQuery);
		}

		return AotQueries.create(aotQuery);
	}

	private RelationalParametersParameterAccessor getAccessor(JdbcQueryMethod queryMethod) {

		JdbcParameters parameters = queryMethod.getParameters();
		Object[] parameterValues = new Object[parameters.getNumberOfParameters()];

		RelationalParameters bindable = parameters.getBindableParameters();
		return PlaceholderAccessor.capture(queryMethod, parameterValues, parameters, bindable);
	}

	private ValueExpressionQueryRewriter.ParsedQuery parseQuery(String queryString) {

		ValueExpressionQueryRewriter rewriter = ValueExpressionQueryRewriter.of(delegate,
				(counter, expression) -> String.format("__$synthetic$__%d", counter + 1), String::concat);

		return rewriter.parse(queryString);
	}

	private List<ParameterBinding> getBindings(ValueExpressionQueryRewriter.ParsedQuery parsedQuery,
			JdbcQueryMethod queryMethod) {

		List<ParameterBinding> bindings = new ArrayList<>();

		queryMethod.getParameters().getBindableParameters().forEach(parameter -> {
			bindings.add(ParameterBinding.of(parameter));
		});

		parsedQuery.getParameterMap().forEach((name, expression) -> {
			bindings.add(ParameterBinding.named(name, ParameterBinding.ParameterOrigin.ofExpression(expression)));
		});

		return bindings;
	}

	/**
	 * Pass-thru implementation for {@link JdbcValue} objects to allow capturing parameter placeholders without applying
	 * conversion.
	 *
	 * @param delegate
	 */
	record AotPassThruJdbcConverter(JdbcConverter delegate) implements JdbcConverter {

		@Override
		public Class<?> getColumnType(RelationalPersistentProperty property) {
			return delegate.getColumnType(property);
		}

		@Override
		public SQLType getTargetSqlType(RelationalPersistentProperty property) {
			return delegate.getTargetSqlType(property);
		}

		@Override
		public RelationalMappingContext getMappingContext() {
			return delegate.getMappingContext();
		}

		@Override
		public ConversionService getConversionService() {
			return delegate.getConversionService();
		}

		@Override
		public EntityInstantiators getEntityInstantiators() {
			return delegate.getEntityInstantiators();
		}

		@Override
		public <T> PersistentPropertyPathAccessor<T> getPropertyAccessor(PersistentEntity<T, ?> persistentEntity,
				T instance) {
			return delegate.getPropertyAccessor(persistentEntity, instance);
		}

		@Override
		public JdbcValue writeJdbcValue(@Nullable Object value, Class<?> type, SQLType sqlType) {
			return value instanceof JdbcValue jdbcValue ? jdbcValue : delegate.writeJdbcValue(value, type, sqlType);
		}

		@Override
		public JdbcValue writeJdbcValue(@Nullable Object value, TypeInformation<?> type, SQLType sqlType) {
			return value instanceof JdbcValue jdbcValue ? jdbcValue : delegate.writeJdbcValue(value, type, sqlType);
		}

		@Override
		public @Nullable Object writeValue(@Nullable Object value, TypeInformation<?> type) {
			return value;
		}

		@Override
		public <R> R readAndResolve(Class<R> type, RowDocument source) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <R> R readAndResolve(Class<R> type, RowDocument source, Identifier identifier) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <R> R readAndResolve(TypeInformation<R> type, RowDocument source, Identifier identifier) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <M, D> EntityProjection<M, D> introspectProjection(Class<M> resultType, Class<D> entityType) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <R> R project(EntityProjection<R, ?> descriptor, RowDocument document) {
			throw new UnsupportedOperationException();
		}

		@Override
		public <R> R read(Class<R> type, RowDocument source) {
			throw new UnsupportedOperationException();
		}

		@Override
		public @Nullable Object readValue(@Nullable Object value, TypeInformation<?> type) {
			throw new UnsupportedOperationException();
		}

	}

}
