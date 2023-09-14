/*
 * Copyright 2018-2023 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.OffsetScrollPosition;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Window;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.ReactiveSelectOperation;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.relational.repository.query.RelationalExampleMapper;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.Streamable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

/**
 * Simple {@link ReactiveSortingRepository} implementation using R2DBC through {@link DatabaseClient}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Mingyuan Wu
 * @author Stephen Cohen
 * @author Greg Turnquist
 */
@Transactional(readOnly = true)
public class SimpleR2dbcRepository<T, ID> implements R2dbcRepository<T, ID> {

	private final RelationalEntityInformation<T, ID> entity;
	private final R2dbcEntityOperations entityOperations;
	private final Lazy<RelationalPersistentProperty> idProperty;
	private final RelationalExampleMapper exampleMapper;

	/**
	 * Create a new {@link SimpleR2dbcRepository}.
	 *
	 * @param entity
	 * @param entityOperations
	 * @param converter
	 * @since 1.1
	 */
	public SimpleR2dbcRepository(RelationalEntityInformation<T, ID> entity, R2dbcEntityOperations entityOperations,
			R2dbcConverter converter) {

		this.entity = entity;
		this.entityOperations = entityOperations;
		this.idProperty = Lazy.of(() -> converter //
				.getMappingContext() //
				.getRequiredPersistentEntity(this.entity.getJavaType()) //
				.getRequiredIdProperty());
		this.exampleMapper = new RelationalExampleMapper(converter.getMappingContext());
	}

	/**
	 * Create a new {@link SimpleR2dbcRepository}.
	 *
	 * @param entity
	 * @param databaseClient
	 * @param converter
	 * @param accessStrategy
	 * @since 1.2
	 */
	public SimpleR2dbcRepository(RelationalEntityInformation<T, ID> entity, DatabaseClient databaseClient,
			R2dbcConverter converter, ReactiveDataAccessStrategy accessStrategy) {

		this.entity = entity;
		this.entityOperations = new R2dbcEntityTemplate(databaseClient, accessStrategy);
		this.idProperty = Lazy.of(() -> converter //
				.getMappingContext() //
				.getRequiredPersistentEntity(this.entity.getJavaType()) //
				.getRequiredIdProperty());
		this.exampleMapper = new RelationalExampleMapper(converter.getMappingContext());
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveCrudRepository
	// -------------------------------------------------------------------------

	@Override
	@Transactional
	public <S extends T> Mono<S> save(S objectToSave) {

		Assert.notNull(objectToSave, "Object to save must not be null");

		if (this.entity.isNew(objectToSave)) {
			return this.entityOperations.insert(objectToSave);
		}

		return this.entityOperations.update(objectToSave);
	}

	@Override
	@Transactional
	public <S extends T> Flux<S> saveAll(Iterable<S> objectsToSave) {

		Assert.notNull(objectsToSave, "Objects to save must not be null");

		return Flux.fromIterable(objectsToSave).concatMap(this::save);
	}

	@Override
	@Transactional
	public <S extends T> Flux<S> saveAll(Publisher<S> objectsToSave) {

		Assert.notNull(objectsToSave, "Object publisher must not be null");

		return Flux.from(objectsToSave).concatMap(this::save);
	}

	@Override
	public Mono<T> findById(ID id) {

		Assert.notNull(id, "Id must not be null");

		return this.entityOperations.selectOne(getIdQuery(id), this.entity.getJavaType());
	}

	@Override
	public Mono<T> findById(Publisher<ID> publisher) {
		return Mono.from(publisher).flatMap(this::findById);
	}

	@Override
	public Mono<Boolean> existsById(ID id) {

		Assert.notNull(id, "Id must not be null");

		return this.entityOperations.exists(getIdQuery(id), this.entity.getJavaType());
	}

	@Override
	public Mono<Boolean> existsById(Publisher<ID> publisher) {
		return Mono.from(publisher).flatMap(this::findById).hasElement();
	}

	@Override
	public Flux<T> findAll() {
		return this.entityOperations.select(Query.empty(), this.entity.getJavaType());
	}

	@Override
	public Flux<T> findAllById(Iterable<ID> iterable) {

		Assert.notNull(iterable, "The iterable of Id's must not be null");

		return findAllById(Flux.fromIterable(iterable));
	}

	@Override
	public Flux<T> findAllById(Publisher<ID> idPublisher) {

		Assert.notNull(idPublisher, "The Id Publisher must not be null");

		return Flux.from(idPublisher).buffer().filter(ids -> !ids.isEmpty()).concatMap(ids -> {

			if (ids.isEmpty()) {
				return Flux.empty();
			}

			String idProperty = getIdProperty().getName();

			return this.entityOperations.select(Query.query(Criteria.where(idProperty).in(ids)), this.entity.getJavaType());
		});
	}

	@Override
	public Mono<Long> count() {
		return this.entityOperations.count(Query.empty(), this.entity.getJavaType());
	}

	@Override
	@Transactional
	public Mono<Void> deleteById(ID id) {

		Assert.notNull(id, "Id must not be null");

		return this.entityOperations.delete(getIdQuery(id), this.entity.getJavaType()).then();
	}

	@Override
	@Transactional
	public Mono<Void> deleteById(Publisher<ID> idPublisher) {

		Assert.notNull(idPublisher, "The Id Publisher must not be null");

		return Flux.from(idPublisher).buffer().filter(ids -> !ids.isEmpty()).concatMap(ids -> {

			if (ids.isEmpty()) {
				return Flux.empty();
			}

			String idProperty = getIdProperty().getName();

			return this.entityOperations.delete(Query.query(Criteria.where(idProperty).in(ids)), this.entity.getJavaType());
		}).then();
	}

	@Override
	@Transactional
	public Mono<Void> delete(T objectToDelete) {

		Assert.notNull(objectToDelete, "Object to delete must not be null");

		return deleteById(this.entity.getRequiredId(objectToDelete));
	}

	@Override
	public Mono<Void> deleteAllById(Iterable<? extends ID> ids) {

		Assert.notNull(ids, "The iterable of Id's must not be null");

		List<? extends ID> idsList = Streamable.of(ids).toList();
		String idProperty = getIdProperty().getName();
		return this.entityOperations.delete(Query.query(Criteria.where(idProperty).in(idsList)), this.entity.getJavaType())
				.then();
	}

	@Override
	@Transactional
	public Mono<Void> deleteAll(Iterable<? extends T> iterable) {

		Assert.notNull(iterable, "The iterable of Id's must not be null");

		return deleteAll(Flux.fromIterable(iterable));
	}

	@Override
	@Transactional
	public Mono<Void> deleteAll(Publisher<? extends T> objectPublisher) {

		Assert.notNull(objectPublisher, "The Object Publisher must not be null");

		Flux<ID> idPublisher = Flux.from(objectPublisher) //
				.map(this.entity::getRequiredId);

		return deleteById(idPublisher);
	}

	@Override
	@Transactional
	public Mono<Void> deleteAll() {
		return this.entityOperations.delete(Query.empty(), this.entity.getJavaType()).then();
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveSortingRepository
	// -------------------------------------------------------------------------

	@Override
	public Flux<T> findAll(Sort sort) {

		Assert.notNull(sort, "Sort must not be null");

		return this.entityOperations.select(Query.empty().sort(sort), this.entity.getJavaType());
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveQueryByExampleExecutor
	// -------------------------------------------------------------------------

	@Override
	public <S extends T> Mono<S> findOne(Example<S> example) {

		Assert.notNull(example, "Example must not be null");

		Query query = this.exampleMapper.getMappedExample(example);

		return this.entityOperations.selectOne(query, example.getProbeType());
	}

	@Override
	public <S extends T> Flux<S> findAll(Example<S> example) {

		Assert.notNull(example, "Example must not be null");

		return findAll(example, Sort.unsorted());
	}

	@Override
	public <S extends T> Flux<S> findAll(Example<S> example, Sort sort) {

		Assert.notNull(example, "Example must not be null");
		Assert.notNull(sort, "Sort must not be null");

		Query query = this.exampleMapper.getMappedExample(example).sort(sort);

		return this.entityOperations.select(query, example.getProbeType());
	}

	@Override
	public <S extends T> Mono<Long> count(Example<S> example) {

		Assert.notNull(example, "Example must not be null");

		Query query = this.exampleMapper.getMappedExample(example);

		return this.entityOperations.count(query, example.getProbeType());
	}

	@Override
	public <S extends T> Mono<Boolean> exists(Example<S> example) {

		Assert.notNull(example, "Example must not be null");

		Query query = this.exampleMapper.getMappedExample(example);

		return this.entityOperations.exists(query, example.getProbeType());
	}

	@Override
	public <S extends T, R, P extends Publisher<R>> P findBy(Example<S> example,
			Function<FluentQuery.ReactiveFluentQuery<S>, P> queryFunction) {

		Assert.notNull(example, "Sample must not be null");
		Assert.notNull(queryFunction, "Query function must not be null");

		return queryFunction.apply(new ReactiveFluentQueryByExample<>(example, example.getProbeType()));
	}

	private RelationalPersistentProperty getIdProperty() {
		return this.idProperty.get();
	}

	private Query getIdQuery(Object id) {
		return Query.query(Criteria.where(getIdProperty().getName()).is(id));
	}

	/**
	 * {@link org.springframework.data.repository.query.FluentQuery.ReactiveFluentQuery} using {@link Example}.
	 *
	 * @author Mark Paluch
	 * @since 1.4
	 */
	class ReactiveFluentQueryByExample<S, T> extends ReactiveFluentQuerySupport<Example<S>, T> {

		ReactiveFluentQueryByExample(Example<S> example, Class<T> resultType) {
			this(example, Sort.unsorted(), 0, resultType, Collections.emptyList());
		}

		ReactiveFluentQueryByExample(Example<S> example, Sort sort, int limit, Class<T> resultType,
				List<String> fieldsToInclude) {
			super(example, sort, limit, resultType, fieldsToInclude);
		}

		@Override
		protected <R> ReactiveFluentQueryByExample<S, R> create(Example<S> predicate, Sort sort, int limit,
				Class<R> resultType, List<String> fieldsToInclude) {
			return new ReactiveFluentQueryByExample<>(predicate, sort, limit, resultType, fieldsToInclude);
		}

		@Override
		public Mono<T> one() {
			return createQuery().one();
		}

		@Override
		public Mono<T> first() {
			return createQuery().first();
		}

		@Override
		public Flux<T> all() {
			return createQuery().all();
		}

		@Override
		public Mono<Window<T>> scroll(ScrollPosition scrollPosition) {

			Assert.notNull(scrollPosition, "ScrollPosition must not be null");

			if (scrollPosition instanceof OffsetScrollPosition osp) {

				int limit = getLimit();
				return createQuery(q -> {

					Query queryToUse = q.offset(osp.getOffset());

					if (limit > 0) {
						queryToUse = queryToUse.limit(limit + 1);
					}

					return queryToUse;
				}).all() //
						.collectList() //
						.map(content -> {
							return ScrollDelegate.createWindow(content, limit,
									OffsetScrollPosition.positionFunction(osp.getOffset()));
						});
			}

			return super.scroll(scrollPosition);
		}

		@Override
		public Mono<Page<T>> page(Pageable pageable) {

			Assert.notNull(pageable, "Pageable must not be null");

			Mono<List<T>> items = createQuery(q -> q.with(pageable)).all().collectList();

			return items.flatMap(content -> ReactivePageableExecutionUtils.getPage(content, pageable, this.count()));
		}

		@Override
		public Mono<Long> count() {
			return createQuery().count();
		}

		@Override
		public Mono<Boolean> exists() {
			return createQuery().exists();
		}

		private ReactiveSelectOperation.TerminatingSelect<T> createQuery() {
			return createQuery(UnaryOperator.identity());
		}

		@SuppressWarnings("unchecked")
		private ReactiveSelectOperation.TerminatingSelect<T> createQuery(UnaryOperator<Query> queryCustomizer) {

			Query query = exampleMapper.getMappedExample(getPredicate());

			if (getSort().isSorted()) {
				query = query.sort(getSort());
			}

			if (!getFieldsToInclude().isEmpty()) {
				query = query.columns(getFieldsToInclude().toArray(new String[0]));
			}

			query = queryCustomizer.apply(query);

			ReactiveSelectOperation.ReactiveSelect<S> select = entityOperations.select(getPredicate().getProbeType());

			if (getResultType() != getPredicate().getProbeType()) {
				return select.as(getResultType()).matching(query);
			}
			return (ReactiveSelectOperation.TerminatingSelect<T>) select.matching(query);
		}
	}
}
