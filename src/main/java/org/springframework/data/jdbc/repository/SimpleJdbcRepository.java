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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.data.jdbc.mapping.event.AfterDeleteEvent;
import org.springframework.data.jdbc.mapping.event.AfterInsertEvent;
import org.springframework.data.jdbc.mapping.event.AfterUpdateEvent;
import org.springframework.data.jdbc.mapping.event.BeforeDeleteEvent;
import org.springframework.data.jdbc.mapping.event.BeforeInsertEvent;
import org.springframework.data.jdbc.mapping.event.BeforeUpdateEvent;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
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
 */
public class SimpleJdbcRepository<T, ID extends Serializable>
		implements CrudRepository<T, ID>, ApplicationEventPublisherAware {

	private final JdbcPersistentEntity<T> entity;
	private final JdbcPersistentEntityInformation<T, ID> entityInformation;
	private final NamedParameterJdbcOperations operations;
	private final SqlGenerator sql;

	private final EntityRowMapper<T> entityRowMapper;
	private final ApplicationEventPublisher publisher;

	public SimpleJdbcRepository(JdbcPersistentEntity<T> persistentEntity, NamedParameterJdbcOperations jdbcOperations, ApplicationEventPublisher publisher) {

		Assert.notNull(persistentEntity, "PersistentEntity must not be null.");
		Assert.notNull(jdbcOperations, "JdbcOperations must not be null.");
		Assert.notNull(publisher, "Publisher must not be null.");

		this.entity = persistentEntity;
		this.entityInformation = new JdbcPersistentEntityInformation<T, ID>(persistentEntity);
		this.operations = jdbcOperations;
		this.publisher = publisher;

		entityRowMapper = new EntityRowMapper<T>(persistentEntity);
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

		entities.forEach(this::save);

		return entities;
	}

	@Override
	public T findOne(ID id) {

		return operations.queryForObject(
				sql.getFindOne(),
				new MapSqlParameterSource("id", id),
				entityRowMapper
		);
	}

	@Override
	public boolean exists(ID id) {

		return operations.queryForObject(
				sql.getExists(),
				new MapSqlParameterSource("id", id),
				Boolean.class
		);
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
		doDelete(id, null);
	}

	@Override
	public void delete(T instance) {

		doDelete((ID) entity.getIdValue(instance), instance);
	}

	@Override
	public void delete(Iterable<? extends T> entities) {

		operations.update(
				sql.getDeleteByList(),
				new MapSqlParameterSource("ids",
						StreamSupport
								.stream(entities.spliterator(), false)
								.map(entity::getIdValue)
								.collect(Collectors.toList())
				)
		);
	}

	@Override
	public void deleteAll() {
		operations.getJdbcOperations().update(sql.getDeleteAll());
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {

	}

	private <S extends T> Map<String, Object> getPropertyMap(final S instance) {

		Map<String, Object> parameters = new HashMap<>();

		this.entity.doWithProperties(new PropertyHandler<JdbcPersistentProperty>() {
			@Override
			public void doWithPersistentProperty(JdbcPersistentProperty persistentProperty) {
				parameters.put(persistentProperty.getColumnName(), entity.getPropertyAccessor(instance).getProperty(persistentProperty));
			}
		});

		return parameters;
	}

	private <S extends T> void doInsert(S instance) {
		publisher.publishEvent(new BeforeInsertEvent(instance, entity::getIdValue));

		KeyHolder holder = new GeneratedKeyHolder();

		operations.update(
				sql.getInsert(),
				new MapSqlParameterSource(getPropertyMap(instance)),
				holder);

		entity.setId(instance, holder.getKey());

		publisher.publishEvent(new AfterInsertEvent(instance, entity::getIdValue));
	}

	private void doDelete(ID id, Object instance) {

		publisher.publishEvent(new BeforeDeleteEvent(instance, o -> id));
		operations.update(sql.getDeleteById(), new MapSqlParameterSource("id", id));
		publisher.publishEvent(new AfterDeleteEvent(instance, o -> id));
	}

	private <S extends T> void doUpdate(S instance) {
		publisher.publishEvent(new BeforeUpdateEvent(instance, entity::getIdValue));

		operations.update(sql.getUpdate(), getPropertyMap(instance));

		publisher.publishEvent(new AfterUpdateEvent(instance, entity::getIdValue));
	}
}
