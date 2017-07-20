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
package org.springframework.data.jdbc.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.data.convert.Jsr310Converters;
import org.springframework.data.jdbc.core.conversion.DbChange;
import org.springframework.data.jdbc.core.conversion.DbChange.Kind;
import org.springframework.data.jdbc.core.conversion.Interpreter;
import org.springframework.data.jdbc.core.conversion.JdbcEntityWriter;
import org.springframework.data.jdbc.core.conversion.JdbcEntityDeleteWriter;
import org.springframework.data.jdbc.mapping.event.AfterDelete;
import org.springframework.data.jdbc.mapping.event.AfterInsert;
import org.springframework.data.jdbc.mapping.event.AfterUpdate;
import org.springframework.data.jdbc.mapping.event.BeforeDelete;
import org.springframework.data.jdbc.mapping.event.BeforeInsert;
import org.springframework.data.jdbc.mapping.event.BeforeUpdate;
import org.springframework.data.jdbc.mapping.event.Identifier;
import org.springframework.data.jdbc.mapping.event.Identifier.Specified;
import org.springframework.data.jdbc.mapping.model.BasicJdbcPersistentEntityInformation;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntityInformation;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.util.Assert;

/**
 * {@link JdbcEntityOperations} implementation, storing complete entities including references in a JDBC data store.
 *
 * @author Jens Schauder
 */
public class JdbcEntityTemplate implements JdbcEntityOperations {

	private static final String ENTITY_NEW_AFTER_INSERT = "Entity [%s] still 'new' after insert. Please set either"
			+ " the id property in a before insert event handler, or ensure the database creates a value and your "
			+ "JDBC driver returns it.";

	private final ApplicationEventPublisher publisher;
	private final NamedParameterJdbcOperations operations;
	private final JdbcMappingContext context;
	private final ConversionService conversions = getDefaultConversionService();
	private final Interpreter interpreter;
	private final SqlGeneratorSource sqlGeneratorSource;

	private final JdbcEntityWriter jdbcConverter;
	private final JdbcEntityDeleteWriter jdbcEntityDeleteConverter;

	public JdbcEntityTemplate(ApplicationEventPublisher publisher, NamedParameterJdbcOperations operations,
			JdbcMappingContext context) {

		this.publisher = publisher;
		this.operations = operations;
		this.context = context;

		jdbcConverter = new JdbcEntityWriter(this.context);
		jdbcEntityDeleteConverter = new JdbcEntityDeleteWriter(this.context);
		sqlGeneratorSource = new SqlGeneratorSource(this.context);
		interpreter = new JdbcInterpreter(this.context, this);
	}

	private static GenericConversionService getDefaultConversionService() {

		DefaultConversionService conversionService = new DefaultConversionService();
		Jsr310Converters.getConvertersToRegister().forEach(conversionService::addConverter);

		return conversionService;
	}

	@Override
	public <T> void save(T instance, Class<T> domainType) {
		createDbChange(instance).executeWith(interpreter);
	}

	@Override
	public <T> void insert(T instance, Class<T> domainType, Map<String, Object> additionalParameter) {

		publisher.publishEvent(new BeforeInsert(instance));

		KeyHolder holder = new GeneratedKeyHolder();
		JdbcPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(domainType);
		JdbcPersistentEntityInformation<T, ?> entityInformation = context
				.getRequiredPersistentEntityInformation(domainType);

		Map<String, Object> propertyMap = getPropertyMap(instance, persistentEntity);

		Object idValue = getIdValueOrNull(instance, persistentEntity);
		JdbcPersistentProperty idProperty = persistentEntity.getRequiredIdProperty();
		propertyMap.put(idProperty.getColumnName(), convert(idValue, idProperty.getColumnType()));

		propertyMap.putAll(additionalParameter);

		operations.update(sql(domainType).getInsert(idValue == null, additionalParameter.keySet()),
				new MapSqlParameterSource(propertyMap), holder);

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
	public <S> void delete(S entity, Class<S> domainType) {

		JdbcPersistentEntityInformation<S, ?> entityInformation = context
				.getRequiredPersistentEntityInformation(domainType);
		deleteTree(entityInformation.getRequiredId(entity), entity, domainType);
	}

	@Override
	public <S> void deleteById(Object id, Class<S> domainType) {
		deleteTree(id, null, domainType);
	}

	@Override
	public void deleteAll(Class<?> domainType) {

		DbChange change = createDeletingDbChange(domainType);
		change.executeWith(interpreter);
	}

	void doDelete(Object rootId, PropertyPath propertyPath) {

		JdbcPersistentEntity<?> entityToDelete = context.getRequiredPersistentEntity(propertyPath.getTypeInformation());

		JdbcPersistentEntity<?> rootEntity = context.getRequiredPersistentEntity(propertyPath.getOwningType());

		JdbcPersistentProperty referencingProperty = rootEntity.getRequiredPersistentProperty(propertyPath.getSegment());
		Assert.notNull(referencingProperty, "No property found matching the PropertyPath " + propertyPath);

		String format = sql(rootEntity.getType()).createDeleteByPath(propertyPath);

		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put("rootId", rootId);
		operations.update(format, parameters);

	}

	void doDelete(Specified specifiedId, Optional<Object> optionalEntity, Class<?> domainType) {

		publisher.publishEvent(new BeforeDelete(specifiedId, optionalEntity));

		String deleteByIdSql = sql(domainType).getDeleteById();

		MapSqlParameterSource parameter = createIdParameterSource(specifiedId.getValue(), domainType);

		operations.update(deleteByIdSql, parameter);

		publisher.publishEvent(new AfterDelete(specifiedId, optionalEntity));
	}

	private void deleteTree(Object id, Object entity, Class<?> domainType) {

		DbChange change = createDeletingDbChange(id, entity, domainType);

		change.executeWith(interpreter);
	}

	private <T> DbChange createDbChange(T instance) {

		DbChange dbChange = new DbChange(Kind.SAVE, instance.getClass(), instance);
		jdbcConverter.write(instance, dbChange);
		return dbChange;
	}

	private DbChange createDeletingDbChange(Object id, Object entity, Class<?> domainType) {

		DbChange dbChange = new DbChange(Kind.DELETE, domainType, entity);
		jdbcEntityDeleteConverter.write(id, dbChange);
		return dbChange;
	}

	private DbChange createDeletingDbChange(Class<?> domainType) {

		DbChange dbChange = new DbChange(Kind.DELETE, domainType, null);
		jdbcEntityDeleteConverter.write(null, dbChange);
		return dbChange;
	}

	private <T> MapSqlParameterSource createIdParameterSource(Object id, Class<T> domainType) {
		return new MapSqlParameterSource("id",
				convert(id, getRequiredPersistentEntity(domainType).getRequiredIdProperty().getColumnType()));
	}

	private <S> Map<String, Object> getPropertyMap(final S instance, JdbcPersistentEntity<S> persistentEntity) {

		Map<String, Object> parameters = new HashMap<>();

		persistentEntity.doWithProperties((PropertyHandler<JdbcPersistentProperty>) property -> {
			if (!property.isEntity()) {
				Object value = persistentEntity.getPropertyAccessor(instance).getProperty(property);

				Object convertedValue = convert(value, property.getColumnType());
				parameters.put(property.getColumnName(), convertedValue);
			}
		});

		return parameters;
	}

	private <S, ID> ID getIdValueOrNull(S instance, JdbcPersistentEntity<S> persistentEntity) {

		EntityInformation<S, ID> entityInformation = new BasicJdbcPersistentEntityInformation<>(persistentEntity);

		ID idValue = entityInformation.getId(instance);

		return isIdPropertySimpleTypeAndValueZero(idValue, persistentEntity) ? null : idValue;
	}

	private <S> void setIdFromJdbc(S instance, KeyHolder holder, JdbcPersistentEntity<S> persistentEntity) {

		JdbcPersistentEntityInformation<S, ?> entityInformation = new BasicJdbcPersistentEntityInformation<>(
				persistentEntity);

		try {

			getIdFromHolder(holder, persistentEntity).ifPresent(it -> {

				Class<?> targetType = persistentEntity.getRequiredIdProperty().getType();
				Object converted = convert(it, targetType);
				entityInformation.setId(instance, converted);
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

		if (from == null) {
			return null;
		}

		JdbcPersistentEntity<?> persistentEntity = context.getPersistentEntity(from.getClass());

		Object id = persistentEntity == null ? null : persistentEntity.getIdentifierAccessor(from).getIdentifier();

		return conversions.convert(id == null ? from : id, to);
	}

	private <S, ID> boolean isIdPropertySimpleTypeAndValueZero(ID idValue, JdbcPersistentEntity<S> persistentEntity) {

		JdbcPersistentProperty idProperty = persistentEntity.getIdProperty();
		return idValue == null //
				|| idProperty == null //
				|| (idProperty.getType() == int.class && idValue.equals(0)) //
				|| (idProperty.getType() == long.class && idValue.equals(0L));
	}

	@SuppressWarnings("unchecked")
	private <S> JdbcPersistentEntity<S> getRequiredPersistentEntity(Class<S> domainType) {
		return (JdbcPersistentEntity<S>) context.getRequiredPersistentEntity(domainType);
	}

	private SqlGenerator sql(Class<?> domainType) {
		return sqlGeneratorSource.getSqlGenerator(domainType);
	}

	private <T> EntityRowMapper<T> getEntityRowMapper(Class<T> domainType) {
		return new EntityRowMapper<>(getRequiredPersistentEntity(domainType), conversions, context);
	}

	<T> void doDeleteAll(Class<T> domainType, PropertyPath propertyPath) {

		operations.getJdbcOperations()
				.update(sql(propertyPath == null ? domainType : propertyPath.getOwningType().getType())
						.createDeleteAllSql(propertyPath));
	}
}
