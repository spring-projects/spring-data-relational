/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.jdbc.repository.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.util.Assert;

/**
 * Support class for {@link FluentQuery.FetchableFluentQuery} implementations.
 *
 * @author Diego Krupitza
 * @author Mark Paluch
 * @since 3.0
 */
abstract class FluentQuerySupport<S, R> implements FluentQuery.FetchableFluentQuery<R> {

	private final Example<S> example;
	private final Sort sort;
	private final int limit;
	private final Class<R> resultType;
	private final List<String> fieldsToInclude;

	private final SpelAwareProxyProjectionFactory projectionFactory = new SpelAwareProxyProjectionFactory();

	FluentQuerySupport(Example<S> example, Sort sort, int limit, Class<R> resultType, List<String> fieldsToInclude) {

		this.example = example;
		this.sort = sort;
		this.limit = limit;
		this.resultType = resultType;
		this.fieldsToInclude = fieldsToInclude;
	}

	@Override
	public FetchableFluentQuery<R> sortBy(Sort sort) {

		Assert.notNull(sort, "Sort must not be null!");

		return create(example, sort, limit, resultType, fieldsToInclude);
	}

	@Override
	public FetchableFluentQuery<R> limit(int limit) {

		Assert.isTrue(limit >= 0, "Limit must not be negative");

		return create(example, sort, limit, resultType, fieldsToInclude);
	}

	@Override
	public <R> FetchableFluentQuery<R> as(Class<R> projection) {

		Assert.notNull(projection, "Projection target type must not be null!");

		return create(example, sort, limit, projection, fieldsToInclude);
	}

	@Override
	public FetchableFluentQuery<R> project(Collection<String> properties) {

		Assert.notNull(properties, "Projection properties must not be null!");

		return create(example, sort, limit, resultType, new ArrayList<>(properties));
	}

	protected abstract <R> FluentQuerySupport<S, R> create(Example<S> example, Sort sort, int limit, Class<R> resultType,
			List<String> fieldsToInclude);

	Class<S> getExampleType() {
		return this.example.getProbeType();
	}

	Example<S> getExample() {
		return this.example;
	}

	Sort getSort() {
		return sort;
	}

	int getLimit() {
		return limit;
	}

	Class<R> getResultType() {
		return resultType;
	}

	List<String> getFieldsToInclude() {
		return fieldsToInclude;
	}

	private Function<Object, R> getConversionFunction(Class<S> inputType, Class<R> targetType) {

		if (targetType.isAssignableFrom(inputType)) {
			return (Function<Object, R>) Function.identity();
		}

		if (targetType.isInterface()) {
			return o -> projectionFactory.createProjection(targetType, o);
		}

		return o -> DefaultConversionService.getSharedInstance().convert(o, targetType);
	}

	protected Function<Object, R> getConversionFunction() {
		return getConversionFunction(this.example.getProbeType(), getResultType());
	}
}
