/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.jdbc.repository;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.data.jdbc.mapping.event.AfterDelete;
import org.springframework.data.jdbc.mapping.event.AfterInsert;
import org.springframework.data.jdbc.mapping.event.AfterUpdate;
import org.springframework.data.jdbc.mapping.event.BeforeDelete;
import org.springframework.data.jdbc.mapping.event.BeforeInsert;
import org.springframework.data.jdbc.mapping.event.BeforeUpdate;
import org.springframework.data.jdbc.mapping.event.Identifier.Specified;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.jdbc.repository.support.BasicJdbcPersistentEntityInformation;
import org.springframework.data.jdbc.repository.support.JdbcPersistentEntityInformation;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.util.Assert;

/**
 * @author Jens Schauder
 * @since 2.0
 */
public class SimpleJdbcRepository<T, ID extends Serializable> implements CrudRepository<T, ID> {

	private static final String ENTITY_NEW_AFTER_INSERT = "Entity [%s] still 'new' after insert. Please set either"
			+ " the id property in a before insert event handler, or ensure the database creates a value and your "
			+ "JDBC driver returns it.";

	private final JdbcPersistentEntity<T> persistentEntity;
	private final JdbcPersistentEntityInformation<T, ID> entityInformation;
	private final NamedParameterJdbcOperations operations;
	private final SqlGenerator sql;

	private final EntityRowMapper<T> entityRowMapper;
	private final ApplicationEventPublisher publisher;

	public SimpleJdbcRepository(JdbcPersistentEntity<T> persistentEntity, NamedParameterJdbcOperations jdbcOperations,
			ApplicationEventPublisher publisher) {

		Assert.notNull(persistentEntity, "PersistentEntity must not be null.");
		Assert.notNull(jdbcOperations, "JdbcOperations must not be null.");
		Assert.notNull(publisher, "Publisher must not be null.");

		this.persistentEntity = persistentEntity;
		this.entityInformation = new BasicJdbcPersistentEntityInformation<>(persistentEntity);
		this.operations = jdbcOperations;
		this.publisher = publisher;

		entityRowMapper = new EntityRowMapper<>(persistentEntity);
		sql = new SqlGenerator(persistentEntity);
	}

	@Override
	public <S extends T> S save(S instance) {

		if (entityInformation.isNew(instance)) {
			doInsert(instance);
		} else {
			doUpdate(instance);
		}

		return instance;
	}

	@Override
	public <S extends T> Iterable<S> save(Iterable<S> entities) {

		List<S> savedEntities = new ArrayList<>();

		entities.forEach(e -> savedEntities.add(save(e)));

		return savedEntities;
	}

	@Override
	public T findOne(ID id) {

		return operations.queryForObject(sql.getFindOne(), new MapSqlParameterSource("id", id), entityRowMapper);
	}

	@Override
	public boolean exists(ID id) {

		return operations.queryForObject(sql.getExists(), new MapSqlParameterSource("id", id), Boolean.class);
	}

	@Override
	public Iterable<T> findAll() {
		return operations.query(sql.getFindAll(), entityRowMapper);
	}

	@Override
	public Iterable<T> findAll(Iterable<ID> ids) {
		return operations.query(sql.getFindAllInList(), new MapSqlParameterSource("ids", ids), entityRowMapper);
	}

	@Override
	public long count() {
		return operations.getJdbcOperations().queryForObject(sql.getCount(), Long.class);
	}

	@Override
	public void delete(ID id) {
		doDelete(new Specified(id), Optional.empty());
	}

	@Override
	public void delete(T instance) {
		doDelete(new Specified(entityInformation.getId(instance)), Optional.of(instance));
	}

	@Override
	public void delete(Iterable<? extends T> entities) {

		operations.update(sql.getDeleteByList(), new MapSqlParameterSource("ids", StreamSupport
				.stream(entities.spliterator(), false).map(entityInformation::getId).collect(Collectors.toList())));
	}

	@Override
	public void deleteAll() {
		operations.getJdbcOperations().update(sql.getDeleteAll());
	}

	private <S extends T> Map<String, Object> getPropertyMap(final S instance) {

		Map<String, Object> parameters = new HashMap<>();

		this.persistentEntity.doWithProperties((PropertyHandler<JdbcPersistentProperty>) //
		property -> parameters.put( //
				property.getColumnName(), //
				persistentEntity.getPropertyAccessor(instance).getProperty(property)) //
		);

		return parameters;
	}

	private <S extends T> void doInsert(S instance) {

		publisher.publishEvent(new BeforeInsert(instance));

		KeyHolder holder = new GeneratedKeyHolder();

		Map<String, Object> propertyMap = getPropertyMap(instance);
		propertyMap.put(persistentEntity.getIdColumn(), getIdValueOrNull(instance));

		operations.update(sql.getInsert(), new MapSqlParameterSource(propertyMap), holder);
		setIdFromJdbc(instance, holder);

		if (entityInformation.isNew(instance)) {
			throw new IllegalStateException(String.format(ENTITY_NEW_AFTER_INSERT, persistentEntity));
		}

		publisher.publishEvent(new AfterInsert(new Specified(entityInformation.getId(instance)), instance));
	}

	private <S extends T> ID getIdValueOrNull(S instance) {

		ID idValue = entityInformation.getId(instance);
		return isIdPropertySimpleTypeAndValueZero(idValue) ? null : idValue;
	}

	private boolean isIdPropertySimpleTypeAndValueZero(ID idValue) {

		return (persistentEntity.getIdProperty().getType() == int.class && idValue.equals(0))
				|| (persistentEntity.getIdProperty().getType() == long.class && idValue.equals(0L));
	}

	private <S extends T> void setIdFromJdbc(S instance, KeyHolder holder) {
		try {
			Number idValueFromJdbc = holder.getKey();
			if (idValueFromJdbc != null) {
				entityInformation.setId(instance, idValueFromJdbc);
			}
		} catch (NonTransientDataAccessException e) {
			throw new UnableToSetIdException("Unable to set id of " + instance, e);
		}
	}

	private void doDelete(Specified specifiedId, Optional<Object> optionalEntity) {

		publisher.publishEvent(new BeforeDelete(specifiedId, optionalEntity));
		operations.update(sql.getDeleteById(), new MapSqlParameterSource("id", specifiedId.getValue()));
		publisher.publishEvent(new AfterDelete(specifiedId, optionalEntity));
	}

	private <S extends T> void doUpdate(S instance) {

		Specified specifiedId = new Specified(entityInformation.getId(instance));
		publisher.publishEvent(new BeforeUpdate(specifiedId, instance));
		operations.update(sql.getUpdate(), getPropertyMap(instance));
		publisher.publishEvent(new AfterUpdate(specifiedId, instance));
	}
}
