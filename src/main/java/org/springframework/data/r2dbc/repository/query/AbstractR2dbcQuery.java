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

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.DatabaseClient.GenericExecuteSpec;
import org.springframework.data.r2dbc.core.FetchSpec;
import org.springframework.data.r2dbc.repository.query.R2dbcQueryExecution.ResultProcessingConverter;
import org.springframework.data.r2dbc.repository.query.R2dbcQueryExecution.ResultProcessingExecution;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.util.Assert;

/**
 * Base class for reactive {@link RepositoryQuery} implementations for R2DBC.
 *
 * @author Mark Paluch
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

		return method.hasReactiveWrapperParameter() ? executeDeferred(parameters)
				: execute(new RelationalParametersParameterAccessor(method, parameters));
	}

	@SuppressWarnings("unchecked")
	private Object executeDeferred(Object[] parameters) {

		R2dbcParameterAccessor parameterAccessor = new R2dbcParameterAccessor(method, parameters);

		if (getQueryMethod().isCollectionQuery()) {
			return Flux.defer(() -> (Publisher<Object>) execute(parameterAccessor));
		}

		return Mono.defer(() -> (Mono<Object>) execute(parameterAccessor));
	}

	private Object execute(RelationalParameterAccessor parameterAccessor) {

		// TODO: ConvertingParameterAccessor
		BindableQuery query = createQuery(parameterAccessor);

		ResultProcessor processor = method.getResultProcessor().withDynamicProjection(parameterAccessor);
		GenericExecuteSpec boundQuery = query.bind(databaseClient.execute(query));
		FetchSpec<?> fetchSpec = boundQuery.as(resolveResultType(processor)).fetch();

		SqlIdentifier tableName = method.getEntityInformation().getTableName();

		R2dbcQueryExecution execution = getExecution(processor.getReturnedType(),
				new ResultProcessingConverter(processor, converter.getMappingContext(), instantiators));

		return execution.execute(fetchSpec, processor.getReturnedType().getDomainType(), tableName);
	}

	private Class<?> resolveResultType(ResultProcessor resultProcessor) {

		ReturnedType returnedType = resultProcessor.getReturnedType();

		return returnedType.isProjecting() ? returnedType.getDomainType() : returnedType.getReturnedType();
	}

	/**
	 * Returns the execution instance to use.
	 *
	 * @param returnedType must not be {@literal null}.
	 * @param resultProcessing must not be {@literal null}.
	 * @return
	 */
	private R2dbcQueryExecution getExecution(ReturnedType returnedType, Converter<Object, Object> resultProcessing) {
		return new ResultProcessingExecution(getExecutionToWrap(returnedType), resultProcessing);
	}

	private R2dbcQueryExecution getExecutionToWrap(ReturnedType returnedType) {

		if (method.isModifyingQuery()) {

			if (Boolean.class.isAssignableFrom(returnedType.getReturnedType())) {
				return (q, t, c) -> q.rowsUpdated().map(integer -> integer > 0);
			}

			if (Number.class.isAssignableFrom(returnedType.getReturnedType())) {

				return (q, t, c) -> q.rowsUpdated().map(integer -> {
					return converter.getConversionService().convert(integer, returnedType.getReturnedType());
				});
			}

			if (Void.class.isAssignableFrom(returnedType.getReturnedType())) {
				return (q, t, c) -> q.rowsUpdated().then();
			}

			return (q, t, c) -> q.rowsUpdated();
		}

		if (method.isCollectionQuery()) {
			return (q, t, c) -> q.all();
		}

		return (q, t, c) -> q.one();
	}

	/**
	 * Creates a {@link BindableQuery} instance using the given {@link ParameterAccessor}
	 *
	 * @param accessor must not be {@literal null}.
	 * @return the {@link BindableQuery}.
	 */
	protected abstract BindableQuery createQuery(RelationalParameterAccessor accessor);
}
