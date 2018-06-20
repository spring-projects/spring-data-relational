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
package org.springframework.data.jdbc.repository.support;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.reactivestreams.Publisher;
import org.springframework.data.jdbc.core.function.DatabaseClient;
import org.springframework.data.jdbc.core.function.DatabaseClient.BindSpec;
import org.springframework.data.jdbc.core.function.DatabaseClient.GenericExecuteSpec;
import org.springframework.data.jdbc.core.function.FetchSpec;
import org.springframework.data.jdbc.core.function.MappingR2dbcConverter;
import org.springframework.data.jdbc.core.mapping.JdbcPersistentEntity;
import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.util.Assert;

/**
 * Simple {@link ReactiveCrudRepository} implementation using R2DBC through {@link DatabaseClient}.
 *
 * @author Mark Paluch
 */
public class SimpleR2dbcRepository<T, ID> implements ReactiveCrudRepository<T, ID> {

	private final DatabaseClient databaseClient;
	private final MappingR2dbcConverter converter;
	private final JdbcPersistentEntity<T> entity;

	/**
	 * Create a new {@link SimpleR2dbcRepository} given {@link DatabaseClient} and {@link JdbcPersistentEntity}.
	 *
	 * @param databaseClient must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 */
	public SimpleR2dbcRepository(DatabaseClient databaseClient, MappingR2dbcConverter converter,
			JdbcPersistentEntity<T> entity) {
		this.converter = converter;

		Assert.notNull(databaseClient, "DatabaseClient must not be null!");
		Assert.notNull(converter, "MappingR2dbcConverter must not be null!");
		Assert.notNull(entity, "PersistentEntity must not be null!");

		this.databaseClient = databaseClient;
		this.entity = entity;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#save(S)
	 */
	@Override
	public <S extends T> Mono<S> save(S objectToSave) {

		Assert.notNull(objectToSave, "Object to save must not be null!");

		if (entity.isNew(objectToSave)) {

			return databaseClient.insert() //
					.into(entity.getType()) //
					.using(objectToSave) //
					.exchange() //
					.flatMap(it -> it.extract(converter.populateIdIfNecessary(objectToSave)).one());
		}

		// TODO: Extract in some kind of SQL generator
		IdentifierAccessor identifierAccessor = entity.getIdentifierAccessor(objectToSave);
		Object id = identifierAccessor.getRequiredIdentifier();

		Map<String, Optional<Object>> fields = converter.getFieldsToUpdate(objectToSave);

		String setClause = getSetClause(fields);

		GenericExecuteSpec exec = databaseClient.execute()
				.sql(String.format("UPDATE %s SET %s WHERE %s = $1", entity.getTableName(), setClause, getIdColumnName())) //
				.bind(0, id);

		int index = 1;
		for (Optional<Object> setValue : fields.values()) {

			Object value = setValue.orElse(null);
			if (value != null) {
				exec = exec.bind(index++, value);
			} else {
				exec = exec.bindNull(index++);
			}
		}

		return exec.as(entity.getType()) //
				.exchange() //
				.flatMap(FetchSpec::rowsUpdated) //
				.thenReturn(objectToSave);
	}

	private static String getSetClause(Map<String, Optional<Object>> fields) {

		StringBuilder setClause = new StringBuilder();

		int index = 2;
		for (String field : fields.keySet()) {

			if (setClause.length() != 0) {
				setClause.append(", ");
			}

			setClause.append(field).append('=').append('$').append(index++);
		}

		return setClause.toString();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#saveAll(java.lang.Iterable)
	 */
	@Override
	public <S extends T> Flux<S> saveAll(Iterable<S> objectsToSave) {

		Assert.notNull(objectsToSave, "Objects to save must not be null!");

		return Flux.fromIterable(objectsToSave).flatMap(this::save);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#saveAll(org.reactivestreams.Publisher)
	 */
	@Override
	public <S extends T> Flux<S> saveAll(Publisher<S> objectsToSave) {

		Assert.notNull(objectsToSave, "Object publisher must not be null!");

		return Flux.from(objectsToSave).flatMap(this::save);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findById(java.lang.Object)
	 */
	@Override
	public Mono<T> findById(ID id) {

		Assert.notNull(id, "Id must not be null!");

		// TODO: Generate proper SQL (select, where clause, parameter binding).
		return databaseClient.execute()
				.sql(String.format("SELECT * FROM %s WHERE %s = $1", entity.getTableName(), getIdColumnName())) //
				.bind("$1", id) //
				.as(entity.getType()) //
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

		// TODO: Generate proper SQL (select, where clause, parameter binding).
		return databaseClient.execute()
				.sql(String.format("SELECT %s FROM %s WHERE %s = $1 LIMIT 1", getIdColumnName(), entity.getTableName(),
						getIdColumnName())) //
				.bind("$1", id) //
				.exchange() //
				.flatMap(it -> it.extract((r, md) -> r).first()).hasElement();
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
		return databaseClient.select().from(entity.getType()).fetch().all();
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

		return Flux.from(idPublisher).buffer().filter(ids -> !ids.isEmpty()).flatMap(ids -> {

			String bindings = getInBinding(ids);

			GenericExecuteSpec exec = databaseClient.execute()
					.sql(String.format("SELECT * FROM %s WHERE %s IN (%s)", entity.getTableName(), getIdColumnName(), bindings));

			return bind(ids, exec).as(entity.getType()).fetch().all();
		});
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#count()
	 */
	@Override
	public Mono<Long> count() {

		return databaseClient.execute()
				.sql(String.format("SELECT COUNT(%s) FROM %s", getIdColumnName(), entity.getTableName())) //
				.exchange() //
				.flatMap(it -> it.extract((r, md) -> r.get(0, Long.class)).first()) //
				.defaultIfEmpty(0L);

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteById(java.lang.Object)
	 */
	@Override
	public Mono<Void> deleteById(ID id) {

		Assert.notNull(id, "Id must not be null!");

		return databaseClient.execute()
				.sql(String.format("DELETE FROM %s WHERE %s = $1", entity.getTableName(), getIdColumnName())) //
				.bind("$1", id) //
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

		return Flux.from(idPublisher).buffer().filter(ids -> !ids.isEmpty()).flatMap(ids -> {

			String bindings = getInBinding(ids);

			GenericExecuteSpec exec = databaseClient.execute()
					.sql(String.format("DELETE FROM %s WHERE %s IN (%s)", entity.getTableName(), getIdColumnName(), bindings));

			return bind(ids, exec).as(entity.getType()).fetch().rowsUpdated();
		}).then();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#delete(java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Mono<Void> delete(T objectToDelete) {

		Assert.notNull(objectToDelete, "Object to delete must not be null!");

		IdentifierAccessor identifierAccessor = entity.getIdentifierAccessor(objectToDelete);

		return deleteById((ID) identifierAccessor.getRequiredIdentifier());
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
	@SuppressWarnings("unchecked")
	public Mono<Void> deleteAll(Publisher<? extends T> objectPublisher) {

		Assert.notNull(objectPublisher, "The Object Publisher must not be null!");

		Flux<ID> idPublisher = Flux.from(objectPublisher) //
				.map(entity::getIdentifierAccessor) //
				.map(identifierAccessor -> (ID) identifierAccessor.getRequiredIdentifier());

		return deleteById(idPublisher);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll()
	 */
	@Override
	public Mono<Void> deleteAll() {

		return databaseClient.execute().sql(String.format("DELETE FROM %s", entity.getTableName())) //
				.exchange() //
				.then();
	}

	private String getInBinding(List<ID> ids) {
		return IntStream.range(1, ids.size() + 1).mapToObj(i -> "$" + i).collect(Collectors.joining(", "));
	}

	@SuppressWarnings("unchecked")
	private <S extends BindSpec<?>> S bind(List<ID> it, S bindSpec) {

		for (int i = 0; i < it.size(); i++) {
			bindSpec = (S) bindSpec.bind(i, it.get(i));
		}

		return bindSpec;
	}

	private String getIdColumnName() {
		return entity.getRequiredIdProperty().getColumnName();
	}
}
