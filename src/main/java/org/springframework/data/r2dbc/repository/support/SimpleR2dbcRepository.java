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
package org.springframework.data.r2dbc.repository.support;

import io.r2dbc.spi.Statement;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.reactivestreams.Publisher;
import org.springframework.data.r2dbc.function.BindIdOperation;
import org.springframework.data.r2dbc.function.BindableOperation;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.data.r2dbc.function.DatabaseClient.GenericExecuteSpec;
import org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.function.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.function.convert.SettableValue;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.util.Assert;

/**
 * Simple {@link ReactiveCrudRepository} implementation using R2DBC through {@link DatabaseClient}.
 *
 * @author Mark Paluch
 */
@RequiredArgsConstructor
public class SimpleR2dbcRepository<T, ID> implements ReactiveCrudRepository<T, ID> {

	private final @NonNull RelationalEntityInformation<T, ID> entity;
	private final @NonNull DatabaseClient databaseClient;
	private final @NonNull MappingR2dbcConverter converter;
	private final @NonNull ReactiveDataAccessStrategy accessStrategy;

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#save(S)
	 */
	@Override
	public <S extends T> Mono<S> save(S objectToSave) {

		Assert.notNull(objectToSave, "Object to save must not be null!");

		if (entity.isNew(objectToSave)) {

			return databaseClient.insert() //
					.into(entity.getJavaType()) //
					.using(objectToSave) //
					.map(converter.populateIdIfNecessary(objectToSave)) //
					.one();
		}

		Object id = entity.getRequiredId(objectToSave);
		Map<String, SettableValue> columns = converter.getColumnsToUpdate(objectToSave);
		columns.remove(getIdColumnName()); // do not update the Id column.
		String idColumnName = getIdColumnName();
		BindIdOperation update = accessStrategy.updateById(entity.getTableName(), columns.keySet(), idColumnName);

		GenericExecuteSpec exec = databaseClient.execute().sql(update);

		BindSpecAdapter<GenericExecuteSpec> wrapper = BindSpecAdapter.create(exec);
		columns.forEach(bind(update, wrapper));
		update.bindId(wrapper, id);

		return wrapper.getBoundOperation().as(entity.getJavaType()) //
				.then() //
				.thenReturn(objectToSave);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#saveAll(java.lang.Iterable)
	 */
	@Override
	public <S extends T> Flux<S> saveAll(Iterable<S> objectsToSave) {

		Assert.notNull(objectsToSave, "Objects to save must not be null!");

		return Flux.fromIterable(objectsToSave).concatMap(this::save);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#saveAll(org.reactivestreams.Publisher)
	 */
	@Override
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

		Set<String> columns = new LinkedHashSet<>(accessStrategy.getAllColumns(entity.getJavaType()));
		String idColumnName = getIdColumnName();
		BindIdOperation select = accessStrategy.selectById(entity.getTableName(), columns, idColumnName);

		GenericExecuteSpec sql = databaseClient.execute().sql(select);
		BindSpecAdapter<GenericExecuteSpec> wrapper = BindSpecAdapter.create(sql);
		select.bindId(wrapper, id);

		return wrapper.getBoundOperation().as(entity.getJavaType()) //
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

		String idColumnName = getIdColumnName();
		BindIdOperation select = accessStrategy.selectById(entity.getTableName(), Collections.singleton(idColumnName),
				idColumnName, 10);

		GenericExecuteSpec sql = databaseClient.execute().sql(select);
		BindSpecAdapter<GenericExecuteSpec> wrapper = BindSpecAdapter.create(sql);
		select.bindId(wrapper, id);

		return wrapper.getBoundOperation().as(entity.getJavaType()) //
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
		return databaseClient.select().from(entity.getJavaType()).fetch().all();
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

			Set<String> columns = new LinkedHashSet<>(accessStrategy.getAllColumns(entity.getJavaType()));
			String idColumnName = getIdColumnName();
			BindIdOperation select = accessStrategy.selectByIdIn(entity.getTableName(), columns, idColumnName);

			BindSpecAdapter<GenericExecuteSpec> wrapper = BindSpecAdapter.create(databaseClient.execute().sql(select));
			select.bindIds(wrapper, ids);

			return wrapper.getBoundOperation().as(entity.getJavaType()).fetch().all();
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#count()
	 */
	@Override
	public Mono<Long> count() {

		return databaseClient.execute()
				.sql(String.format("SELECT COUNT(%s) FROM %s", getIdColumnName(), entity.getTableName())) //
				.map((r, md) -> r.get(0, Long.class)) //
				.first() //
				.defaultIfEmpty(0L);

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteById(java.lang.Object)
	 */
	@Override
	public Mono<Void> deleteById(ID id) {

		Assert.notNull(id, "Id must not be null!");

		BindIdOperation delete = accessStrategy.deleteById(entity.getTableName(), getIdColumnName());
		BindSpecAdapter<GenericExecuteSpec> wrapper = BindSpecAdapter.create(databaseClient.execute().sql(delete));

		delete.bindId(wrapper, id);

		return wrapper.getBoundOperation() //
				.fetch() //
				.rowsUpdated() //
				.then();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteById(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<Void> deleteById(Publisher<ID> idPublisher) {

		Assert.notNull(idPublisher, "The Id Publisher must not be null!");

		return Flux.from(idPublisher).buffer().filter(ids -> !ids.isEmpty()).concatMap(ids -> {

			if (ids.isEmpty()) {
				return Flux.empty();
			}

			String idColumnName = getIdColumnName();
			BindIdOperation delete = accessStrategy.deleteByIdIn(entity.getTableName(), idColumnName);

			BindSpecAdapter<GenericExecuteSpec> wrapper = BindSpecAdapter.create(databaseClient.execute().sql(delete));
			delete.bindIds(wrapper, ids);

			return wrapper.getBoundOperation().as(entity.getJavaType()).fetch().rowsUpdated();
		}).then();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#delete(java.lang.Object)
	 */
	@Override
	public Mono<Void> delete(T objectToDelete) {

		Assert.notNull(objectToDelete, "Object to delete must not be null!");

		return deleteById(entity.getRequiredId(objectToDelete));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll(java.lang.Iterable)
	 */
	@Override
	public Mono<Void> deleteAll(Iterable<? extends T> iterable) {

		Assert.notNull(iterable, "The iterable of Id's must not be null!");

		return deleteAll(Flux.fromIterable(iterable));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll(org.reactivestreams.Publisher)
	 */
	@Override
	public Mono<Void> deleteAll(Publisher<? extends T> objectPublisher) {

		Assert.notNull(objectPublisher, "The Object Publisher must not be null!");

		Flux<ID> idPublisher = Flux.from(objectPublisher) //
				.map(entity::getRequiredId);

		return deleteById(idPublisher);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll()
	 */
	@Override
	public Mono<Void> deleteAll() {

		return databaseClient.execute().sql(String.format("DELETE FROM %s", entity.getTableName())) //
				.then();
	}

	private String getIdColumnName() {

		return converter //
				.getMappingContext() //
				.getRequiredPersistentEntity(entity.getJavaType()) //
				.getRequiredIdProperty() //
				.getColumnName();
	}

	private BiConsumer<String, SettableValue> bind(BindableOperation operation, Statement<?> statement) {

		return (k, v) -> operation.bind(statement, v);
	}
}
