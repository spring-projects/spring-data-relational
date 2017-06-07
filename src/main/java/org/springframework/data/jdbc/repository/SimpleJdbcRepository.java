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

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.data.jdbc.mapping.event.AfterDelete;
import org.springframework.data.jdbc.mapping.event.AfterInsert;
import org.springframework.data.jdbc.mapping.event.AfterUpdate;
import org.springframework.data.jdbc.mapping.event.BeforeDelete;
import org.springframework.data.jdbc.mapping.event.BeforeInsert;
import org.springframework.data.jdbc.mapping.event.BeforeUpdate;
import org.springframework.data.jdbc.mapping.event.Identifier;
import org.springframework.data.jdbc.mapping.event.Identifier.Specified;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntityImpl;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.jdbc.repository.support.BasicJdbcPersistentEntityInformation;
import org.springframework.data.jdbc.repository.support.JdbcPersistentEntityInformation;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.util.Streamable;
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
	private final ConversionService conversions = new DefaultConversionService();

	/**
	 * Creates a new {@link SimpleJdbcRepository} for the given {@link JdbcPersistentEntityImpl}
	 * 
	 * @param persistentEntity
	 * @param jdbcOperations
	 * @param publisher
	 */
	public SimpleJdbcRepository(JdbcPersistentEntity<T> persistentEntity, NamedParameterJdbcOperations jdbcOperations,
			ApplicationEventPublisher publisher) {

		Assert.notNull(persistentEntity, "PersistentEntity must not be null.");
		Assert.notNull(jdbcOperations, "JdbcOperations must not be null.");
		Assert.notNull(publisher, "Publisher must not be null.");

		this.persistentEntity = persistentEntity;
		this.entityInformation = new BasicJdbcPersistentEntityInformation<>(persistentEntity);
		this.operations = jdbcOperations;
		this.publisher = publisher;

		this.entityRowMapper = new EntityRowMapper<>(persistentEntity);
		this.sql = new SqlGenerator(persistentEntity);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(S)
	 */
	@Override
	public <S extends T> S save(S instance) {

		if (entityInformation.isNew(instance)) {
			doInsert(instance);
		} else {
			doUpdate(instance);
		}

		return instance;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Iterable)
	 */
	@Override
	public <S extends T> Iterable<S> saveAll(Iterable<S> entities) {

		List<S> savedEntities = new ArrayList<>();
		entities.forEach(e -> savedEntities.add(save(e)));
		return savedEntities;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findOne(java.io.Serializable)
	 */
	@Override
	public Optional<T> findById(ID id) {

		return Optional
				.ofNullable(operations.queryForObject(sql.getFindOne(), new MapSqlParameterSource("id", id), entityRowMapper));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#exists(java.io.Serializable)
	 */
	@Override
	public boolean existsById(ID id) {
		return operations.queryForObject(sql.getExists(), new MapSqlParameterSource("id", id), Boolean.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll()
	 */
	@Override
	public Iterable<T> findAll() {
		return operations.query(sql.getFindAll(), entityRowMapper);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll(java.lang.Iterable)
	 */
	@Override
	public Iterable<T> findAllById(Iterable<ID> ids) {
		return operations.query(sql.getFindAllInList(), new MapSqlParameterSource("ids", ids), entityRowMapper);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#count()
	 */
	@Override
	public long count() {
		return operations.getJdbcOperations().queryForObject(sql.getCount(), Long.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.io.Serializable)
	 */
	@Override
	public void deleteById(ID id) {
		doDelete(Identifier.of(id), Optional.empty());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Object)
	 */
	@Override
	public void delete(T instance) {
		doDelete(Identifier.of(entityInformation.getRequiredId(instance)), Optional.of(instance));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Iterable)
	 */
	@Override
	public void deleteAll(Iterable<? extends T> entities) {

		List<ID> idList = Streamable.of(entities).stream() //
				.map(e -> entityInformation.getRequiredId(e)) //
				.collect(Collectors.toList());

		MapSqlParameterSource sqlParameterSource = new MapSqlParameterSource("ids", idList);
		operations.update(sql.getDeleteByList(), sqlParameterSource);
	}

	@Override
	public void deleteAll() {
		operations.getJdbcOperations().update(sql.getDeleteAll());
	}

	private <S extends T> Map<String, Object> getPropertyMap(final S instance) {

		Map<String, Object> parameters = new HashMap<>();

		this.persistentEntity.doWithProperties((PropertyHandler<JdbcPersistentProperty>) property -> {

			Optional<Object> value = persistentEntity.getPropertyAccessor(instance).getProperty(property);
			parameters.put(property.getColumnName(), value.orElse(null));
		});

		return parameters;
	}

	private <S extends T> void doInsert(S instance) {

		publisher.publishEvent(new BeforeInsert(instance));

		KeyHolder holder = new GeneratedKeyHolder();

		Map<String, Object> propertyMap = getPropertyMap(instance);
		propertyMap.put(persistentEntity.getRequiredIdProperty().getColumnName(), getIdValueOrNull(instance));

		operations.update(sql.getInsert(), new MapSqlParameterSource(propertyMap), holder);
		setIdFromJdbc(instance, holder);

		if (entityInformation.isNew(instance)) {
			throw new IllegalStateException(String.format(ENTITY_NEW_AFTER_INSERT, persistentEntity));
		}

		publisher.publishEvent(new AfterInsert(Identifier.of(entityInformation.getRequiredId(instance)), instance));
	}

	private <S extends T> ID getIdValueOrNull(S instance) {

		Optional<ID> idValue = entityInformation.getId(instance);
		return isIdPropertySimpleTypeAndValueZero(idValue) ? null : idValue.get();
	}

	private boolean isIdPropertySimpleTypeAndValueZero(Optional<ID> idValue) {

		Optional<JdbcPersistentProperty> idProperty = persistentEntity.getIdProperty();
		return !idValue.isPresent() //
				|| !idProperty.isPresent() //
				|| (((Optional<JdbcPersistentProperty>) idProperty).get().getType() == int.class && idValue.equals(0)) //
				|| (((Optional<JdbcPersistentProperty>) idProperty).get().getType() == long.class && idValue.equals(0L));
	}

	private <S extends T> void setIdFromJdbc(S instance, KeyHolder holder) {

		try {

			getIdFromHolder(holder).ifPresent(it -> {

				Class<?> targetType = persistentEntity.getRequiredIdProperty().getType();
				Object converted = convert(it, targetType);
				entityInformation.setId(instance, Optional.of(converted));
			});

		} catch (NonTransientDataAccessException e) {
			throw new UnableToSetId("Unable to set id of " + instance, e);
		}
	}

	private Optional<Object> getIdFromHolder(KeyHolder holder) {

		try {
			// MySQL just returns one value with a special name
			return Optional.ofNullable(holder.getKey());
		} catch (InvalidDataAccessApiUsageException e) {
			// Postgres returns a value for each column
			return Optional.ofNullable(holder.getKeys().get(persistentEntity.getIdColumn()));
		}

	}

	private <V> V convert(Object idValueFromJdbc, Class<V> targetType) {
		return conversions.convert(idValueFromJdbc, targetType);
	}

	private void doDelete(Specified specifiedId, Optional<Object> optionalEntity) {

		publisher.publishEvent(new BeforeDelete(specifiedId, optionalEntity));
		operations.update(sql.getDeleteById(), new MapSqlParameterSource("id", specifiedId.getValue()));
		publisher.publishEvent(new AfterDelete(specifiedId, optionalEntity));
	}

	private <S extends T> void doUpdate(S instance) {

		Specified specifiedId = Identifier.of(entityInformation.getRequiredId(instance));
		publisher.publishEvent(new BeforeUpdate(specifiedId, instance));
		operations.update(sql.getUpdate(), getPropertyMap(instance));
		publisher.publishEvent(new AfterUpdate(specifiedId, instance));
	}
}
