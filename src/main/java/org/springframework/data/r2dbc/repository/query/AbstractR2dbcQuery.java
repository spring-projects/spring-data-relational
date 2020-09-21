/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.r2dbc.repository.query;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;

import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.r2dbc.convert.EntityRowMapper;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.repository.query.R2dbcQueryExecution.ResultProcessingConverter;
import org.springframework.data.r2dbc.repository.query.R2dbcQueryExecution.ResultProcessingExecution;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.FetchSpec;
import org.springframework.r2dbc.core.RowsFetchSpec;
import org.springframework.util.Assert;

/**
 * Base class for reactive {@link RepositoryQuery} implementations for R2DBC.
 *
 * @author Mark Paluch
 * @author Stephen Cohen
 */
public abstract class AbstractR2dbcQuery implements RepositoryQuery {

	private final R2dbcQueryMethod method;
	private final DatabaseClient databaseClient;
	private final R2dbcConverter converter;
	private final EntityInstantiators instantiators;

	/**
	 * Creates a new {@link AbstractR2dbcQuery} from the given {@link R2dbcQueryMethod} and {@link DatabaseClient}.
	 *
	 * @param method must not be {@literal null}.
	 * @param databaseClient must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public AbstractR2dbcQuery(R2dbcQueryMethod method, DatabaseClient databaseClient, R2dbcConverter converter) {

		Assert.notNull(method, "R2dbcQueryMethod must not be null!");
		Assert.notNull(databaseClient, "DatabaseClient must not be null!");
		Assert.notNull(converter, "R2dbcConverter must not be null!");

		this.method = method;
		this.databaseClient = databaseClient;
		this.converter = converter;
		this.instantiators = new EntityInstantiators();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	public R2dbcQueryMethod getQueryMethod() {
		return method;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	public Object execute(Object[] parameters) {

		RelationalParameterAccessor parameterAccessor = new RelationalParametersParameterAccessor(method, parameters);

		return createQuery(parameterAccessor).flatMapMany(it -> executeQuery(parameterAccessor, it));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Publisher<?> executeQuery(RelationalParameterAccessor parameterAccessor, BindableQuery it) {

		ResultProcessor processor = method.getResultProcessor().withDynamicProjection(parameterAccessor);
		DatabaseClient.GenericExecuteSpec boundQuery = it.bind(databaseClient.sql(it));

		FetchSpec<Object> fetchSpec;

		if (isExistsQuery()) {
			fetchSpec = (FetchSpec) boundQuery.map(row -> true);
		} else if (requiresMapping()) {
			EntityRowMapper rowMapper = new EntityRowMapper<>(resolveResultType(processor), converter);
			fetchSpec = new FetchSpecAdapter<>(boundQuery.map(rowMapper));
		} else {
			fetchSpec = (FetchSpec) boundQuery.fetch();
		}

		SqlIdentifier tableName = method.getEntityInformation().getTableName();

		R2dbcQueryExecution execution = new ResultProcessingExecution(getExecutionToWrap(processor.getReturnedType()),
				new ResultProcessingConverter(processor, converter.getMappingContext(), instantiators));

		return execution.execute(fetchSpec, processor.getReturnedType().getDomainType(), tableName);
	}

	private boolean requiresMapping() {
		return !isModifyingQuery();
	}

	private Class<?> resolveResultType(ResultProcessor resultProcessor) {

		ReturnedType returnedType = resultProcessor.getReturnedType();

		return returnedType.isProjecting() ? returnedType.getDomainType() : returnedType.getReturnedType();
	}

	private R2dbcQueryExecution getExecutionToWrap(ReturnedType returnedType) {

		if (isModifyingQuery()) {

			if (Boolean.class.isAssignableFrom(returnedType.getReturnedType())) {
				return (q, t, c) -> q.rowsUpdated().map(integer -> integer > 0);
			}

			if (Number.class.isAssignableFrom(returnedType.getReturnedType())) {

				return (q, t, c) -> q.rowsUpdated().map(integer -> {
					return converter.getConversionService().convert(integer, returnedType.getReturnedType());
				});
			}

			if (ReflectionUtils.isVoid(returnedType.getReturnedType())) {
				return (q, t, c) -> q.rowsUpdated().then();
			}

			return (q, t, c) -> q.rowsUpdated();
		}

		if (isCountQuery()) {
			return (q, t, c) -> q.first().defaultIfEmpty(0L);
		}

		if (isExistsQuery()) {
			return (q, t, c) -> q.first().defaultIfEmpty(false);
		}

		if (method.isCollectionQuery()) {
			return (q, t, c) -> q.all();
		}

		return (q, t, c) -> q.one();
	}

	/**
	 * Returns whether this query is a modifying one.
	 *
	 * @return
	 * @since 1.1
	 */
	protected abstract boolean isModifyingQuery();

	/**
	 * Returns whether the query should get a count projection applied.
	 *
	 * @return
	 * @since 1.2
	 */
	protected abstract boolean isCountQuery();

	/**
	 * Returns whether the query should get an exists projection applied.
	 *
	 * @return
	 * @since 1.2
	 */
	protected abstract boolean isExistsQuery();

	/**
	 * Creates a {@link BindableQuery} instance using the given {@link ParameterAccessor}
	 *
	 * @param accessor must not be {@literal null}.
	 * @return a mono emitting a {@link BindableQuery}.
	 */
	protected abstract Mono<BindableQuery> createQuery(RelationalParameterAccessor accessor);

	private static class FetchSpecAdapter<T> implements FetchSpec<T> {

		private final RowsFetchSpec<T> delegate;

		private FetchSpecAdapter(RowsFetchSpec<T> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Mono<T> one() {
			return delegate.one();
		}

		@Override
		public Mono<T> first() {
			return delegate.first();
		}

		@Override
		public Flux<T> all() {
			return delegate.all();
		}

		@Override
		public Mono<Integer> rowsUpdated() {
			throw new UnsupportedOperationException("Not supported after applying a row mapper");
		}
	}
}
