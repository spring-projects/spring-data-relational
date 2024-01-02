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
package org.springframework.data.r2dbc.repository.query;

import reactor.core.publisher.Mono;

import org.reactivestreams.Publisher;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.repository.query.R2dbcQueryExecution.ResultProcessingConverter;
import org.springframework.data.r2dbc.repository.query.R2dbcQueryExecution.ResultProcessingExecution;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.r2dbc.core.FetchSpec;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.RowsFetchSpec;
import org.springframework.util.Assert;

/**
 * Base class for reactive {@link RepositoryQuery} implementations for R2DBC.
 *
 * @author Mark Paluch
 * @author Stephen Cohen
 * @author Christoph Strobl
 */
public abstract class AbstractR2dbcQuery implements RepositoryQuery {

	private final R2dbcQueryMethod method;
	private final R2dbcEntityOperations entityOperations;
	private final R2dbcConverter converter;
	private final EntityInstantiators instantiators;

	/**
	 * Creates a new {@link AbstractR2dbcQuery} from the given {@link R2dbcQueryMethod} and {@link R2dbcEntityOperations}.
	 *
	 * @param method must not be {@literal null}.
	 * @param entityOperations must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @since 1.4
	 */
	public AbstractR2dbcQuery(R2dbcQueryMethod method, R2dbcEntityOperations entityOperations, R2dbcConverter converter) {

		Assert.notNull(method, "R2dbcQueryMethod must not be null");
		Assert.notNull(entityOperations, "R2dbcEntityOperations must not be null");
		Assert.notNull(converter, "R2dbcConverter must not be null");

		this.method = method;
		this.entityOperations = entityOperations;
		this.converter = converter;
		this.instantiators = new EntityInstantiators();
	}

	public R2dbcQueryMethod getQueryMethod() {
		return method;
	}

	public Object execute(Object[] parameters) {

		Mono<R2dbcParameterAccessor> resolveParameters = new R2dbcParameterAccessor(method, parameters).resolveParameters();
		return resolveParameters.flatMapMany(it -> createQuery(it).flatMapMany(foo -> executeQuery(it, foo)));
	}

	@SuppressWarnings("unchecked")
	private Publisher<?> executeQuery(R2dbcParameterAccessor parameterAccessor, PreparedOperation<?> operation) {

		ResultProcessor processor = method.getResultProcessor().withDynamicProjection(parameterAccessor);

		RowsFetchSpec<?> fetchSpec;

		if (isModifyingQuery()) {
			fetchSpec = entityOperations.getDatabaseClient().sql(operation).fetch();
		} else if (isExistsQuery()) {
			fetchSpec = entityOperations.getDatabaseClient().sql(operation).map(row -> true);
		} else {
			fetchSpec = entityOperations.query(operation, processor.getReturnedType()
							.getDomainType(),
					resolveResultType(processor));
		}

		R2dbcQueryExecution execution = new ResultProcessingExecution(getExecutionToWrap(processor.getReturnedType()),
				new ResultProcessingConverter(processor, converter.getMappingContext(), instantiators));

		return execution.execute((RowsFetchSpec) fetchSpec);
	}

	Class<?> resolveResultType(ResultProcessor resultProcessor) {

		ReturnedType returnedType = resultProcessor.getReturnedType();

		if (returnedType.getReturnedType().isAssignableFrom(returnedType.getDomainType())) {
			return returnedType.getDomainType();
		}

		return returnedType.getReturnedType();
	}

	private R2dbcQueryExecution getExecutionToWrap(ReturnedType returnedType) {

		if (isModifyingQuery()) {

			return fetchSpec -> {

				Assert.isInstanceOf(FetchSpec.class, fetchSpec);

				FetchSpec<?> fs = (FetchSpec<?>) fetchSpec;

				if (Boolean.class.isAssignableFrom(returnedType.getReturnedType())) {
					return fs.rowsUpdated().map(integer -> integer > 0);
				}

				if (Number.class.isAssignableFrom(returnedType.getReturnedType())) {

					return fs.rowsUpdated()
							.map(count -> converter.getConversionService().convert(count, returnedType.getReturnedType()));
				}

				if (ReflectionUtils.isVoid(returnedType.getReturnedType())) {
					return fs.rowsUpdated().then();
				}

				return fs.rowsUpdated();
			};
		}

		if (isCountQuery()) {
			return (fetchSpec) -> fetchSpec.first().defaultIfEmpty(0L);
		}

		if (isExistsQuery()) {
			return (fetchSpec) -> fetchSpec.first().defaultIfEmpty(false);
		}

		if (method.isCollectionQuery()) {
			return RowsFetchSpec::all;
		}

		return RowsFetchSpec::one;
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
	protected abstract Mono<PreparedOperation<?>> createQuery(RelationalParameterAccessor accessor);

}
