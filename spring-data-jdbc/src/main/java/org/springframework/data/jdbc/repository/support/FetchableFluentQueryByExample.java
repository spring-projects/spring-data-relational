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
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.data.domain.Example;
import org.springframework.data.domain.OffsetScrollPosition;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.repository.query.RelationalExampleMapper;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.data.repository.query.FluentQuery.FetchableFluentQuery} using {@link Example}.
 *
 * @author Diego Krupitza
 * @author Mark Paluch
 * @since 3.0
 */
class FetchableFluentQueryByExample<S, R> extends FluentQuerySupport<S, R> {

	private final RelationalExampleMapper exampleMapper;
	private final JdbcAggregateOperations entityOperations;

	FetchableFluentQueryByExample(Example<S> example, Class<R> resultType, RelationalExampleMapper exampleMapper,
			JdbcAggregateOperations entityOperations) {
		this(example, Sort.unsorted(), 0, resultType, Collections.emptyList(), exampleMapper, entityOperations);
	}

	FetchableFluentQueryByExample(Example<S> example, Sort sort, int limit, Class<R> resultType,
			List<String> fieldsToInclude, RelationalExampleMapper exampleMapper, JdbcAggregateOperations entityOperations) {

		super(example, sort, limit, resultType, fieldsToInclude);

		this.exampleMapper = exampleMapper;
		this.entityOperations = entityOperations;
	}

	@Override
	public R oneValue() {

		return this.entityOperations.findOne(createQuery(), getExampleType())
				.map(item -> this.getConversionFunction().apply(item)).get();
	}

	@Override
	public R firstValue() {

		return this.getConversionFunction()
				.apply(this.entityOperations.findAll(createQuery().sort(getSort()), getExampleType()).iterator().next());
	}

	@Override
	public List<R> all() {
		return findAll(createQuery().sort(getSort()));
	}

	private List<R> findAll(Query query) {

		Function<Object, R> conversionFunction = this.getConversionFunction();
		Iterable<S> raw = this.entityOperations.findAll(query, getExampleType());

		List<R> result = new ArrayList<>(raw instanceof Collections ? ((Collection<?>) raw).size() : 16);

		for (S s : raw) {
			result.add(conversionFunction.apply(s));
		}

		return result;
	}

	@Override
	public Window<R> scroll(ScrollPosition scrollPosition) {

		Assert.notNull(scrollPosition, "ScrollPosition must not be null");

		if (scrollPosition instanceof OffsetScrollPosition osp) {

			Query query = createQuery().sort(getSort());

			if (!osp.isInitial()) {
				query = query.offset(osp.getOffset() + 1);
			}

			if (getLimit() > 0) {
				query = query.limit(getLimit());
			}

			return ScrollDelegate.scroll(query, this::findAll, osp);
		}

		return super.scroll(scrollPosition);
	}

	@Override
	public Page<R> page(Pageable pageable) {

		return this.entityOperations.findAll(createQuery(p -> p.with(pageable)), getExampleType(), pageable)
				.map(item -> this.getConversionFunction().apply(item));
	}

	@Override
	public Stream<R> stream() {

		return StreamSupport
				.stream(this.entityOperations.findAll(createQuery().sort(getSort()), getExampleType()).spliterator(), false)
				.map(item -> this.getConversionFunction().apply(item));
	}

	@Override
	public long count() {
		return this.entityOperations.count(createQuery(), getExampleType());
	}

	@Override
	public boolean exists() {
		return this.entityOperations.exists(createQuery(), getExampleType());
	}

	private Query createQuery() {
		return createQuery(UnaryOperator.identity());
	}

	private Query createQuery(UnaryOperator<Query> queryCustomizer) {

		Query query = exampleMapper.getMappedExample(getExample());

		if (!getFieldsToInclude().isEmpty()) {
			query = query.columns(getFieldsToInclude().toArray(new String[0]));
		}

		query = query.limit(getLimit());

		query = queryCustomizer.apply(query);

		return query;
	}

	@Override
	protected <R> FluentQuerySupport<S, R> create(Example<S> example, Sort sort, int limit, Class<R> resultType,
			List<String> fieldsToInclude) {

		return new FetchableFluentQueryByExample<>(example, sort, limit, resultType, fieldsToInclude, this.exampleMapper,
				this.entityOperations);
	}
}
