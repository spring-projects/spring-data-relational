/*
 * Copyright 2018-2021 the original author or authors.
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

import java.util.List;

import org.reactivestreams.Publisher;

import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
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
 */
@Transactional(readOnly = true)
public class SimpleR2dbcRepository<T, ID> implements ReactiveSortingRepository<T, ID> {

	private final RelationalEntityInformation<T, ID> entity;
	private final R2dbcEntityOperations entityOperations;
	private final Lazy<RelationalPersistentProperty> idProperty;

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
	}

	/**
	 * Create a new {@link SimpleR2dbcRepository}.
	 *
	 * @param entity
	 * @param databaseClient
	 * @param converter
	 * @param accessStrategy
	 * @deprecated since 1.2.
	 */
	@Deprecated
	public SimpleR2dbcRepository(RelationalEntityInformation<T, ID> entity,
			org.springframework.data.r2dbc.core.DatabaseClient databaseClient, R2dbcConverter converter,
			ReactiveDataAccessStrategy accessStrategy) {

		this.entity = entity;
		this.entityOperations = new R2dbcEntityTemplate(databaseClient, accessStrategy);
		this.idProperty = Lazy.of(() -> converter //
				.getMappingContext() //
				.getRequiredPersistentEntity(this.entity.getJavaType()) //
				.getRequiredIdProperty());
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveCrudRepository
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#save(S)
	 */
	@Override
	@Transactional
	public <S extends T> Mono<S> save(S objectToSave) {

		Assert.notNull(objectToSave, "Object to save must not be null!");

		if (this.entity.isNew(objectToSave)) {
			return this.entityOperations.insert(objectToSave);
		}

		return this.entityOperations.update(objectToSave);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#saveAll(java.lang.Iterable)
	 */
	@Override
	@Transactional
	public <S extends T> Flux<S> saveAll(Iterable<S> objectsToSave) {

		Assert.notNull(objectsToSave, "Objects to save must not be null!");

		return Flux.fromIterable(objectsToSave).concatMap(this::save);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#saveAll(org.reactivestreams.Publisher)
	 */
	@Override
	@Transactional
	public <S extends T> Flux<S> saveAll(Publisher<S> objectsToSave) {

		Assert.notNull(objectsToSave, "Object publisher must not be null!");

		return Flux.from(objectsToSave).concatMap(this::save);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findById(java.lang.Object)
	 */
	@Override
	public Mono<T> findById(ID id) {

		Assert.notNull(id, "Id must not be null!");

		return this.entityOperations.selectOne(getIdQuery(id), this.entity.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findById(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<T> findById(Publisher<ID> publisher) {
		return Mono.from(publisher).flatMap(this::findById);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#existsById(java.lang.Object)
	 */
	@Override
	public Mono<Boolean> existsById(ID id) {

		Assert.notNull(id, "Id must not be null!");

		return this.entityOperations.exists(getIdQuery(id), this.entity.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#existsById(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<Boolean> existsById(Publisher<ID> publisher) {
		return Mono.from(publisher).flatMap(this::findById).hasElement();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAll()
	 */
	@Override
	public Flux<T> findAll() {
		return this.entityOperations.select(Query.empty(), this.entity.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAllById(java.lang.Iterable)
	 */
	@Override
	public Flux<T> findAllById(Iterable<ID> iterable) {

		Assert.notNull(iterable, "The iterable of Id's must not be null!");

		return findAllById(Flux.fromIterable(iterable));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAllById(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<T> findAllById(Publisher<ID> idPublisher) {

		Assert.notNull(idPublisher, "The Id Publisher must not be null!");

		return Flux.from(idPublisher).buffer().filter(ids -> !ids.isEmpty()).concatMap(ids -> {

			if (ids.isEmpty()) {
				return Flux.empty();
			}

			String idProperty = getIdProperty().getName();

			return this.entityOperations.select(Query.query(Criteria.where(idProperty).in(ids)), this.entity.getJavaType());
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#count()
	 */
	@Override
	public Mono<Long> count() {
		return this.entityOperations.count(Query.empty(), this.entity.getJavaType());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteById(java.lang.Object)
	 */
	@Override
	@Transactional
	public Mono<Void> deleteById(ID id) {

		Assert.notNull(id, "Id must not be null!");

		return this.entityOperations.delete(getIdQuery(id), this.entity.getJavaType()).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteById(org.reactivestreams.Publisher)
	 */
	@Override
	@Transactional
	public Mono<Void> deleteById(Publisher<ID> idPublisher) {

		Assert.notNull(idPublisher, "The Id Publisher must not be null!");

		return Flux.from(idPublisher).buffer().filter(ids -> !ids.isEmpty()).concatMap(ids -> {

			if (ids.isEmpty()) {
				return Flux.empty();
			}

			String idProperty = getIdProperty().getName();

			return this.entityOperations.delete(Query.query(Criteria.where(idProperty).in(ids)), this.entity.getJavaType());
		}).then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#delete(java.lang.Object)
	 */
	@Override
	@Transactional
	public Mono<Void> delete(T objectToDelete) {

		Assert.notNull(objectToDelete, "Object to delete must not be null!");

		return deleteById(this.entity.getRequiredId(objectToDelete));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAllById(java.lang.Iterable)
	 */
	@Override
	public Mono<Void> deleteAllById(Iterable<? extends ID> ids) {

		Assert.notNull(ids, "The iterable of Id's must not be null!");

		List<? extends ID> idsList = Streamable.of(ids).toList();
		String idProperty = getIdProperty().getName();
		return this.entityOperations.delete(Query.query(Criteria.where(idProperty).in(idsList)), this.entity.getJavaType())
				.then();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll(java.lang.Iterable)
	 */
	@Override
	@Transactional
	public Mono<Void> deleteAll(Iterable<? extends T> iterable) {

		Assert.notNull(iterable, "The iterable of Id's must not be null!");

		return deleteAll(Flux.fromIterable(iterable));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll(org.reactivestreams.Publisher)
	 */
	@Override
	@Transactional
	public Mono<Void> deleteAll(Publisher<? extends T> objectPublisher) {

		Assert.notNull(objectPublisher, "The Object Publisher must not be null!");

		Flux<ID> idPublisher = Flux.from(objectPublisher) //
				.map(this.entity::getRequiredId);

		return deleteById(idPublisher);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll()
	 */
	@Override
	@Transactional
	public Mono<Void> deleteAll() {
		return this.entityOperations.delete(Query.empty(), this.entity.getJavaType()).then();
	}

	// -------------------------------------------------------------------------
	// Methods from ReactiveSortingRepository
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveSortingRepository#findAll(org.springframework.data.domain.Sort)
	 */
	@Override
	public Flux<T> findAll(Sort sort) {

		Assert.notNull(sort, "Sort must not be null!");

		return this.entityOperations.select(Query.empty().sort(sort), this.entity.getJavaType());
	}

	private RelationalPersistentProperty getIdProperty() {
		return this.idProperty.get();
	}

	private Query getIdQuery(Object id) {
		return Query.query(Criteria.where(getIdProperty().getName()).is(id));
	}
}
