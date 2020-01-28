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
package org.springframework.data.r2dbc.repository.support;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import org.reactivestreams.Publisher;

import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.PreparedOperation;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.StatementMapper;
import org.springframework.data.r2dbc.query.Criteria;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.StatementBuilder;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

/**
 * Simple {@link ReactiveCrudRepository} implementation using R2DBC through {@link DatabaseClient}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
@Transactional(readOnly = true)
public class SimpleR2dbcRepository<T, ID> implements ReactiveCrudRepository<T, ID> {

	private final RelationalEntityInformation<T, ID> entity;
	private final DatabaseClient databaseClient;
	private final R2dbcConverter converter;
	private final ReactiveDataAccessStrategy accessStrategy;

	public SimpleR2dbcRepository(RelationalEntityInformation<T, ID> entity, DatabaseClient databaseClient,
			R2dbcConverter converter, ReactiveDataAccessStrategy accessStrategy) {
		this.entity = entity;
		this.databaseClient = databaseClient;
		this.converter = converter;
		this.accessStrategy = accessStrategy;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#save(S)
	 */
	@Override
	@Transactional
	public <S extends T> Mono<S> save(S objectToSave) {

		Assert.notNull(objectToSave, "Object to save must not be null!");

		if (this.entity.isNew(objectToSave)) {

			return this.databaseClient.insert() //
					.into(this.entity.getJavaType()) //
					.table(this.entity.getTableName()).using(objectToSave) //
					.map(this.converter.populateIdIfNecessary(objectToSave)) //
					.first() //
					.defaultIfEmpty(objectToSave);
		}

		return this.databaseClient.update() //
				.table(this.entity.getJavaType()) //
				.table(this.entity.getTableName()).using(objectToSave) //
				.fetch().rowsUpdated().handle((rowsUpdated, sink) -> {

					if (rowsUpdated == 0) {
						sink.error(new TransientDataAccessResourceException(
								String.format("Failed to update table [%s]. Row with Id [%s] does not exist.",
										this.entity.getTableName(), this.entity.getId(objectToSave))));
					} else {
						sink.next(objectToSave);
					}
				});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#saveAll(java.lang.Iterable)
	 */
	@Override
	@Transactional
	public <S extends T> Flux<S> saveAll(Iterable<S> objectsToSave) {

		Assert.notNull(objectsToSave, "Objects to save must not be null!");

		return Flux.fromIterable(objectsToSave).concatMap(this::save);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#saveAll(org.reactivestreams.Publisher)
	 */
	@Override
	@Transactional
	public <S extends T> Flux<S> saveAll(Publisher<S> objectsToSave) {

		Assert.notNull(objectsToSave, "Object publisher must not be null!");

		return Flux.from(objectsToSave).concatMap(this::save);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findById(java.lang.Object)
	 */
	@Override
	public Mono<T> findById(ID id) {

		Assert.notNull(id, "Id must not be null!");

		List<SqlIdentifier> columns = this.accessStrategy.getAllColumns(this.entity.getJavaType());
		String idProperty = getIdProperty().getName();

		StatementMapper mapper = this.accessStrategy.getStatementMapper().forType(this.entity.getJavaType());
		StatementMapper.SelectSpec selectSpec = mapper.createSelect(this.entity.getTableName()) //
				.withProjection(columns) //
				.withCriteria(Criteria.where(idProperty).is(id));

		PreparedOperation<?> operation = mapper.getMappedObject(selectSpec);

		return this.databaseClient.execute(operation) //
				.as(this.entity.getJavaType()) //
				.fetch() //
				.one();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findById(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<T> findById(Publisher<ID> publisher) {
		return Mono.from(publisher).flatMap(this::findById);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#existsById(java.lang.Object)
	 */
	@Override
	public Mono<Boolean> existsById(ID id) {

		Assert.notNull(id, "Id must not be null!");

		String idProperty = getIdProperty().getName();

		StatementMapper mapper = this.accessStrategy.getStatementMapper().forType(this.entity.getJavaType());
		StatementMapper.SelectSpec selectSpec = mapper.createSelect(this.entity.getTableName()).withProjection(idProperty) //
				.withCriteria(Criteria.where(idProperty).is(id));

		PreparedOperation<?> operation = mapper.getMappedObject(selectSpec);

		return this.databaseClient.execute(operation) //
				.map((r, md) -> r) //
				.first() //
				.hasElement();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#existsById(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<Boolean> existsById(Publisher<ID> publisher) {
		return Mono.from(publisher).flatMap(this::findById).hasElement();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAll()
	 */
	@Override
	public Flux<T> findAll() {
		return this.databaseClient.select().from(this.entity.getJavaType()).fetch().all();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAllById(java.lang.Iterable)
	 */
	@Override
	public Flux<T> findAllById(Iterable<ID> iterable) {

		Assert.notNull(iterable, "The iterable of Id's must not be null!");

		return findAllById(Flux.fromIterable(iterable));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAllById(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<T> findAllById(Publisher<ID> idPublisher) {

		Assert.notNull(idPublisher, "The Id Publisher must not be null!");

		return Flux.from(idPublisher).buffer().filter(ids -> !ids.isEmpty()).concatMap(ids -> {

			if (ids.isEmpty()) {
				return Flux.empty();
			}

			List<SqlIdentifier> columns = this.accessStrategy.getAllColumns(this.entity.getJavaType());
			String idProperty = getIdProperty().getName();

			StatementMapper mapper = this.accessStrategy.getStatementMapper().forType(this.entity.getJavaType());
			StatementMapper.SelectSpec selectSpec = mapper.createSelect(this.entity.getTableName()) //
					.withProjection(columns) //
					.withCriteria(Criteria.where(idProperty).in(ids));

			PreparedOperation<?> operation = mapper.getMappedObject(selectSpec);

			return this.databaseClient.execute(operation).as(this.entity.getJavaType()).fetch().all();
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#count()
	 */
	@Override
	public Mono<Long> count() {

		Table table = Table.create(this.accessStrategy.toSql(this.entity.getTableName()));
		Select select = StatementBuilder //
				.select(Functions.count(table.column(this.accessStrategy.toSql(getIdProperty().getColumnName())))) //
				.from(table) //
				.build();

		return this.databaseClient.execute(SqlRenderer.toString(select)) //
				.map((r, md) -> r.get(0, Long.class)) //
				.first() //
				.defaultIfEmpty(0L);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteById(java.lang.Object)
	 */
	@Override
	@Transactional
	public Mono<Void> deleteById(ID id) {

		Assert.notNull(id, "Id must not be null!");

		return this.databaseClient.delete() //
				.from(this.entity.getJavaType()) //
				.table(this.entity.getTableName()) //
				.matching(Criteria.where(getIdProperty().getName()).is(id)) //
				.fetch() //
				.rowsUpdated() //
				.then();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteById(org.reactivestreams.Publisher)
	 */
	@Override
	@Transactional
	public Mono<Void> deleteById(Publisher<ID> idPublisher) {

		Assert.notNull(idPublisher, "The Id Publisher must not be null!");
		StatementMapper statementMapper = this.accessStrategy.getStatementMapper().forType(this.entity.getJavaType());

		return Flux.from(idPublisher).buffer().filter(ids -> !ids.isEmpty()).concatMap(ids -> {

			if (ids.isEmpty()) {
				return Flux.empty();
			}

			return this.databaseClient.delete() //
					.from(this.entity.getJavaType()) //
					.table(this.entity.getTableName()) //
					.matching(Criteria.where(getIdProperty().getName()).in(ids)) //
					.fetch() //
					.rowsUpdated();
		}).then();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#delete(java.lang.Object)
	 */
	@Override
	@Transactional
	public Mono<Void> delete(T objectToDelete) {

		Assert.notNull(objectToDelete, "Object to delete must not be null!");

		return deleteById(this.entity.getRequiredId(objectToDelete));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll(java.lang.Iterable)
	 */
	@Override
	@Transactional
	public Mono<Void> deleteAll(Iterable<? extends T> iterable) {

		Assert.notNull(iterable, "The iterable of Id's must not be null!");

		return deleteAll(Flux.fromIterable(iterable));
	}

	/* (non-Javadoc)
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

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll()
	 */
	@Override
	@Transactional
	public Mono<Void> deleteAll() {
		return this.databaseClient.delete().from(this.entity.getTableName()).then();
	}

	private RelationalPersistentProperty getIdProperty() {

		return this.converter //
				.getMappingContext() //
				.getRequiredPersistentEntity(this.entity.getJavaType()) //
				.getRequiredIdProperty();
	}
}
