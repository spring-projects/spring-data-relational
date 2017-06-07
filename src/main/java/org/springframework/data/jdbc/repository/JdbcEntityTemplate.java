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

import lombok.RequiredArgsConstructor;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.data.convert.Jsr310Converters;
import org.springframework.data.jdbc.mapping.context.JdbcMappingContext;
import org.springframework.data.jdbc.mapping.event.AfterDelete;
import org.springframework.data.jdbc.mapping.event.AfterInsert;
import org.springframework.data.jdbc.mapping.event.AfterUpdate;
import org.springframework.data.jdbc.mapping.event.BeforeDelete;
import org.springframework.data.jdbc.mapping.event.BeforeInsert;
import org.springframework.data.jdbc.mapping.event.BeforeUpdate;
import org.springframework.data.jdbc.mapping.event.Identifier;
import org.springframework.data.jdbc.mapping.event.Identifier.Specified;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.jdbc.repository.support.BasicJdbcPersistentEntityInformation;
import org.springframework.data.jdbc.repository.support.JdbcPersistentEntityInformation;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.util.Streamable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * @author Jens Schauder
 */
@RequiredArgsConstructor
public class JdbcEntityTemplate implements JdbcEntityOperations {

	private static final String ENTITY_NEW_AFTER_INSERT = "Entity [%s] still 'new' after insert. Please set either"
			+ " the id property in a before insert event handler, or ensure the database creates a value and your "
			+ "JDBC driver returns it.";

	private final ApplicationEventPublisher publisher;
	private final NamedParameterJdbcOperations operations;
	private final JdbcMappingContext context;
	private final ConversionService conversions = getDefaultConversionService();

	private static GenericConversionService getDefaultConversionService() {

		DefaultConversionService conversionService = new DefaultConversionService();
		Jsr310Converters.getConvertersToRegister().forEach(conversionService::addConverter);

		return conversionService;
	}

	@Override
	public <S> void insert(S instance, Class<S> domainType) {

		publisher.publishEvent(new BeforeInsert(instance));

		KeyHolder holder = new GeneratedKeyHolder();
		JdbcPersistentEntity<S> persistentEntity = getRequiredPersistentEntity(domainType);
		JdbcPersistentEntityInformation<S, ?> entityInformation = context
				.getRequiredPersistentEntityInformation(domainType);

		Map<String, Object> propertyMap = getPropertyMap(instance, persistentEntity);

		Object idValue = getIdValueOrNull(instance, persistentEntity);
		JdbcPersistentProperty idProperty = persistentEntity.getRequiredIdProperty();
		propertyMap.put(idProperty.getColumnName(), convert(idValue, idProperty.getColumnType()));

		operations.update(sql(domainType).getInsert(idValue == null), new MapSqlParameterSource(propertyMap), holder);

		setIdFromJdbc(instance, holder, persistentEntity);

		if (entityInformation.isNew(instance)) {
			throw new IllegalStateException(String.format(ENTITY_NEW_AFTER_INSERT, persistentEntity));
		}

		publisher.publishEvent(new AfterInsert(Identifier.of(entityInformation.getRequiredId(instance)), instance));

	}

	@Override
	public <S> void update(S instance, Class<S> domainType) {

		JdbcPersistentEntity<S> persistentEntity = getRequiredPersistentEntity(domainType);
		JdbcPersistentEntityInformation<S, ?> entityInformation = context
				.getRequiredPersistentEntityInformation(domainType);

		Specified specifiedId = Identifier.of(entityInformation.getRequiredId(instance));
		publisher.publishEvent(new BeforeUpdate(specifiedId, instance));
		operations.update(sql(domainType).getUpdate(), getPropertyMap(instance, persistentEntity));
		publisher.publishEvent(new AfterUpdate(specifiedId, instance));
	}

	@Override
	public <S> void delete(S entity, Class<S> domainType) {

		JdbcPersistentEntityInformation<S, ?> entityInformation = context
				.getRequiredPersistentEntityInformation(domainType);
		delete(Identifier.of(entityInformation.getRequiredId(entity)), Optional.of(entity), domainType);
	}

	@Override
	public <S> void deleteById(Object id, Class<S> domainType) {
		delete(Identifier.of(id), Optional.empty(), domainType);
	}

	@Override
	public long count(Class<?> domainType) {
		return operations.getJdbcOperations().queryForObject(sql(domainType).getCount(), Long.class);
	}

	@Override
	public <T> T findById(Object id, Class<T> domainType) {

		String findOneSql = sql(domainType).getFindOne();
		MapSqlParameterSource parameter = createIdParameterSource(id, domainType);
		return operations.queryForObject(findOneSql, parameter, getEntityRowMapper(domainType));
	}

	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {

		String existsSql = sql(domainType).getExists();
		MapSqlParameterSource parameter = createIdParameterSource(id, domainType);
		return operations.queryForObject(existsSql, parameter, Boolean.class);
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {
		return operations.query(sql(domainType).getFindAll(), getEntityRowMapper(domainType));
	}

	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {

		String findAllInListSql = sql(domainType).getFindAllInList();
		Class<?> targetType = getRequiredPersistentEntity(domainType).getRequiredIdProperty().getColumnType();
		MapSqlParameterSource parameter = new MapSqlParameterSource("ids",
				StreamSupport.stream(ids.spliterator(), false).map(id -> convert(id, targetType)).collect(Collectors.toList()));

		return operations.query(findAllInListSql, parameter, getEntityRowMapper(domainType));
	}

	@Override
	public <T> void deleteAll(Iterable<? extends T> entities, Class<T> domainType) {

		JdbcPersistentEntityInformation<T, ?> entityInformation = context
				.getRequiredPersistentEntityInformation(domainType);

		Class<?> targetType = context.getRequiredPersistentEntity(domainType).getRequiredIdProperty().getColumnType();
		List<?> idList = Streamable.of(entities).stream() //
				.map(entityInformation::getRequiredId) //
				.map(id -> convert(id, targetType))
				.collect(Collectors.toList());

		MapSqlParameterSource sqlParameterSource = new MapSqlParameterSource("ids", idList);
		operations.update(sql(domainType).getDeleteByList(), sqlParameterSource);
	}

	@Override
	public void deleteAll(Class<?> domainType) {
		operations.getJdbcOperations().update(sql(domainType).getDeleteAll());
	}

	private void delete(Specified specifiedId, Optional<Object> optionalEntity, Class<?> domainType) {

		publisher.publishEvent(new BeforeDelete(specifiedId, optionalEntity));

		String deleteByIdSql = sql(domainType).getDeleteById();

		MapSqlParameterSource parameter = createIdParameterSource(specifiedId.getValue(), domainType);

		operations.update(deleteByIdSql, parameter);

		publisher.publishEvent(new AfterDelete(specifiedId, optionalEntity));
	}

	private <T> MapSqlParameterSource createIdParameterSource(Object id, Class<T> domainType) {
		return new MapSqlParameterSource("id",
				convert(id, getRequiredPersistentEntity(domainType).getRequiredIdProperty().getColumnType()));
	}

	private <S> Map<String, Object> getPropertyMap(final S instance, JdbcPersistentEntity<S> persistentEntity) {

		Map<String, Object> parameters = new HashMap<>();

		persistentEntity.doWithProperties((PropertyHandler<JdbcPersistentProperty>) property -> {

			Optional<Object> value = persistentEntity.getPropertyAccessor(instance).getProperty(property);

			Object convertedValue = convert(value.orElse(null), property.getColumnType());
			parameters.put(property.getColumnName(), convertedValue);
		});

		return parameters;
	}

	private <S, ID> ID getIdValueOrNull(S instance, JdbcPersistentEntity<S> persistentEntity) {

		EntityInformation<S, ID> entityInformation = new BasicJdbcPersistentEntityInformation<>(persistentEntity);

		Optional<ID> idValue = entityInformation.getId(instance);

		return isIdPropertySimpleTypeAndValueZero(idValue, persistentEntity) ? null
				: idValue.orElseThrow(() -> new IllegalStateException("idValue must have a value at this point."));
	}

	private <S> void setIdFromJdbc(S instance, KeyHolder holder, JdbcPersistentEntity<S> persistentEntity) {

		JdbcPersistentEntityInformation<S, ?> entityInformation = new BasicJdbcPersistentEntityInformation<>(
				persistentEntity);

		try {

			getIdFromHolder(holder, persistentEntity).ifPresent(it -> {

				Class<?> targetType = persistentEntity.getRequiredIdProperty().getType();
				Object converted = convert(it, targetType);
				entityInformation.setId(instance, Optional.of(converted));
			});

		} catch (NonTransientDataAccessException e) {
			throw new UnableToSetId("Unable to set id of " + instance, e);
		}
	}

	private <S> Optional<Object> getIdFromHolder(KeyHolder holder, JdbcPersistentEntity<S> persistentEntity) {

		try {
			// MySQL just returns one value with a special name
			return Optional.ofNullable(holder.getKey());
		} catch (InvalidDataAccessApiUsageException e) {
			// Postgres returns a value for each column
			return Optional.ofNullable(holder.getKeys().get(persistentEntity.getIdColumn()));
		}
	}

	private <V> V convert(Object from, Class<V> to) {
		return conversions.convert(from, to);
	}

	private <S, ID> boolean isIdPropertySimpleTypeAndValueZero(Optional<ID> idValue,
			JdbcPersistentEntity<S> persistentEntity) {

		Optional<JdbcPersistentProperty> idProperty = persistentEntity.getIdProperty();
		return !idValue.isPresent() //
				|| !idProperty.isPresent() //
				|| (idProperty.get().getType() == int.class && idValue.get().equals(0)) //
				|| (idProperty.get().getType() == long.class && idValue.get().equals(0L));
	}

	@SuppressWarnings("unchecked")
	private <S> JdbcPersistentEntity<S> getRequiredPersistentEntity(Class<S> domainType) {
		return (JdbcPersistentEntity<S>) context.getRequiredPersistentEntity(domainType);
	}

	private SqlGenerator sql(Class<?> domainType) {
		return new SqlGenerator(context.getRequiredPersistentEntity(domainType));
	}

	private <T> EntityRowMapper<T> getEntityRowMapper(Class<T> domainType) {
		return new EntityRowMapper<>(getRequiredPersistentEntity(domainType), conversions);
	}
}
