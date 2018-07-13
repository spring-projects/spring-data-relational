/*
 * Copyright 2017-2018 the original author or authors.
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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * The default {@link DataAccessStrategy} is to generate SQL statements based on meta data from the entity.
 *
 * @author Jens Schauder
 * @since 1.0
 */
@RequiredArgsConstructor
public class DefaultDataAccessStrategy implements DataAccessStrategy {

	private static final String ENTITY_NEW_AFTER_INSERT = "Entity [%s] still 'new' after insert. Please set either"
			+ " the id property in a BeforeInsert event handler, or ensure the database creates a value and your "
			+ "JDBC driver returns it.";

	private final @NonNull SqlGeneratorSource sqlGeneratorSource;
	private final @NonNull RelationalMappingContext context;
	private final @NonNull NamedParameterJdbcOperations operations;
	private final @NonNull EntityInstantiators instantiators;
	private final @NonNull DataAccessStrategy accessStrategy;

	/**
	 * Creates a {@link DefaultDataAccessStrategy} which references it self for resolution of recursive data accesses.
	 * Only suitable if this is the only access strategy in use.
	 */
	public DefaultDataAccessStrategy(SqlGeneratorSource sqlGeneratorSource, RelationalMappingContext context,
			NamedParameterJdbcOperations operations, EntityInstantiators instantiators) {

		this.sqlGeneratorSource = sqlGeneratorSource;
		this.operations = operations;
		this.context = context;
		this.instantiators = instantiators;
		this.accessStrategy = this;
	}

	@Override
	public <T> void insert(PersistentPropertyAccessor<T> instance, Class<T> domainType,
			Map<String, Object> additionalParameters) {

		KeyHolder holder = new GeneratedKeyHolder();
		RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(domainType);

		MapSqlParameterSource parameterSource = getPropertyMap(instance, persistentEntity);

		Object idValue = getIdValueOrNull(instance.getBean(), persistentEntity);
		RelationalPersistentProperty idProperty = persistentEntity.getIdProperty();

		if (idValue != null) {

			Assert.notNull(idProperty, "Since we have a non-null idValue, we must have an idProperty as well.");

			additionalParameters.put(idProperty.getColumnName(), convert(idValue, idProperty.getColumnType()));
		}

		additionalParameters.forEach(parameterSource::addValue);

		operations.update( //
				sql(domainType).getInsert(additionalParameters.keySet()), //
				parameterSource, //
				holder //
		);

		setIdFromJdbc(instance, holder, persistentEntity);

		// if there is an id property and it was null before the save
		// The database should have created an id and provided it.

		if (idProperty != null && idValue == null && persistentEntity.isNew(instance)) {
			throw new IllegalStateException(String.format(ENTITY_NEW_AFTER_INSERT, persistentEntity));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#update(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <S> void update(PersistentPropertyAccessor<S> instance, Class<S> domainType) {

		RelationalPersistentEntity<S> persistentEntity = getRequiredPersistentEntity(domainType);

		operations.update(sql(domainType).getUpdate(), getPropertyMap(instance, persistentEntity));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#delete(java.lang.Object, java.lang.Class)
	 */
	@Override
	public void delete(Object id, Class<?> domainType) {

		String deleteByIdSql = sql(domainType).getDeleteById();
		MapSqlParameterSource parameter = createIdParameterSource(id, domainType);

		operations.update(deleteByIdSql, parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#delete(java.lang.Object, org.springframework.data.mapping.PropertyPath)
	 */
	@Override
	public void delete(Object rootId, PropertyPath propertyPath) {

		RelationalPersistentEntity<?> rootEntity = context.getRequiredPersistentEntity(propertyPath.getOwningType());

		RelationalPersistentProperty referencingProperty = rootEntity
				.getRequiredPersistentProperty(propertyPath.getSegment());
		Assert.notNull(referencingProperty, "No property found matching the PropertyPath " + propertyPath);

		String format = sql(rootEntity.getType()).createDeleteByPath(propertyPath);

		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put("rootId", rootId);
		operations.update(format, parameters);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteAll(java.lang.Class)
	 */
	@Override
	public <T> void deleteAll(Class<T> domainType) {
		operations.getJdbcOperations().update(sql(domainType).createDeleteAllSql(null));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteAll(org.springframework.data.mapping.PropertyPath)
	 */
	@Override
	public void deleteAll(PropertyPath propertyPath) {
		operations.getJdbcOperations().update(sql(propertyPath.getOwningType().getType()).createDeleteAllSql(propertyPath));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#count(java.lang.Class)
	 */
	@Override
	public long count(Class<?> domainType) {

		Long result = operations.getJdbcOperations().queryForObject(sql(domainType).getCount(), Long.class);

		Assert.notNull(result, "The result of a count query must not be null.");

		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findById(java.lang.Object, java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> T findById(Object id, Class<T> domainType) {

		String findOneSql = sql(domainType).getFindOne();
		MapSqlParameterSource parameter = createIdParameterSource(id, domainType);

		try {
			return operations.queryForObject(findOneSql, parameter, (RowMapper<T>) getEntityRowMapper(domainType));
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAll(java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {
		return operations.query(sql(domainType).getFindAll(), (RowMapper<T>) getEntityRowMapper(domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAllById(java.lang.Iterable, java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {

		String findAllInListSql = sql(domainType).getFindAllInList();
		Class<?> targetType = getRequiredPersistentEntity(domainType).getRequiredIdProperty().getColumnType();

		MapSqlParameterSource parameter = new MapSqlParameterSource( //
				"ids", //
				StreamSupport.stream(ids.spliterator(), false) //
						.map(id -> convert(id, targetType)) //
						.collect(Collectors.toList()) //
		);

		return operations.query(findAllInListSql, parameter, (RowMapper<T>) getEntityRowMapper(domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAllByProperty(java.lang.Object, org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> Iterable<T> findAllByProperty(Object rootId, RelationalPersistentProperty property) {

		Assert.notNull(rootId, "rootId must not be null.");

		Class<?> actualType = property.getActualType();
		String findAllByProperty = sql(actualType) //
				.getFindAllByProperty(property.getReverseColumnName(), property.getKeyColumn(), property.isOrdered());

		MapSqlParameterSource parameter = new MapSqlParameterSource(property.getReverseColumnName(), rootId);

		return operations.query(findAllByProperty, parameter, //
				(RowMapper<T>) (property.isMap() //
						? this.getMapEntityRowMapper(property) //
						: this.getEntityRowMapper(actualType)));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#existsById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {

		String existsSql = sql(domainType).getExists();
		MapSqlParameterSource parameter = createIdParameterSource(id, domainType);

		Boolean result = operations.queryForObject(existsSql, parameter, Boolean.class);

		Assert.notNull(result, "The result of an exists query must not be null");

		return result;
	}

	private <S> MapSqlParameterSource getPropertyMap(final PersistentPropertyAccessor<S> instance,
			RelationalPersistentEntity<S> persistentEntity) {

		MapSqlParameterSource parameters = new MapSqlParameterSource();

		persistentEntity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) property -> {
			if (!property.isEntity()) {

				Object value = instance.getProperty(property);

				Object convertedValue = convert(value, property.getColumnType());
				parameters.addValue(property.getColumnName(), convertedValue, JdbcUtil.sqlTypeFor(property.getColumnType()));
			}
		});

		return parameters;
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private <S, ID> ID getIdValueOrNull(S instance, RelationalPersistentEntity<S> persistentEntity) {

		ID idValue = (ID) persistentEntity.getIdentifierAccessor(instance).getIdentifier();

		return isIdPropertyNullOrScalarZero(idValue, persistentEntity) ? null : idValue;
	}

	private static <S, ID> boolean isIdPropertyNullOrScalarZero(@Nullable ID idValue,
			RelationalPersistentEntity<S> persistentEntity) {

		RelationalPersistentProperty idProperty = persistentEntity.getIdProperty();
		return idValue == null //
				|| idProperty == null //
				|| (idProperty.getType() == int.class && idValue.equals(0)) //
				|| (idProperty.getType() == long.class && idValue.equals(0L));
	}

	private <S> void setIdFromJdbc(PersistentPropertyAccessor<S> accessor, KeyHolder holder,
			RelationalPersistentEntity<S> persistentEntity) {

		try {

			getIdFromHolder(holder, persistentEntity).ifPresent(it -> {

				ConvertingPropertyAccessor convertingPropertyAccessor = new ConvertingPropertyAccessor(accessor,
						context.getConversions());
				RelationalPersistentProperty idProperty = persistentEntity.getRequiredIdProperty();

				convertingPropertyAccessor.setProperty(idProperty, it);
			});

		} catch (NonTransientDataAccessException e) {
			throw new UnableToSetId("Unable to set id of " + accessor.getBean(), e);
		}
	}

	private <S> Optional<Object> getIdFromHolder(KeyHolder holder, RelationalPersistentEntity<S> persistentEntity) {

		try {
			// MySQL just returns one value with a special name
			return Optional.ofNullable(holder.getKey());
		} catch (InvalidDataAccessApiUsageException e) {
			// Postgres returns a value for each column
			Map<String, Object> keys = holder.getKeys();
			return Optional.ofNullable(keys == null ? null : keys.get(persistentEntity.getIdColumn()));
		}
	}

	public EntityRowMapper<?> getEntityRowMapper(Class<?> domainType) {
		return new EntityRowMapper<>(getRequiredPersistentEntity(domainType), context, instantiators, accessStrategy);
	}

	@SuppressWarnings("unchecked")
	private RowMapper<?> getMapEntityRowMapper(RelationalPersistentProperty property) {

		String keyColumn = property.getKeyColumn();

		Assert.notNull(keyColumn, () -> "keyColumn must not be null for " + property);

		return new MapEntityRowMapper<>(getEntityRowMapper(property.getActualType()), keyColumn);
	}

	private <T> MapSqlParameterSource createIdParameterSource(Object id, Class<T> domainType) {

		Class<?> columnType = getRequiredPersistentEntity(domainType).getRequiredIdProperty().getColumnType();
		return new MapSqlParameterSource("id", convert(id, columnType));
	}

	@SuppressWarnings("unchecked")
	private <S> RelationalPersistentEntity<S> getRequiredPersistentEntity(Class<S> domainType) {
		return (RelationalPersistentEntity<S>) context.getRequiredPersistentEntity(domainType);
	}

	@Nullable
	private <V> V convert(@Nullable Object from, Class<V> to) {

		if (from == null) {
			return null;
		}

		RelationalPersistentEntity<?> persistentEntity = context.getPersistentEntity(from.getClass());

		Object id = persistentEntity == null ? null : persistentEntity.getIdentifierAccessor(from).getIdentifier();

		return context.getConversions().convert(id == null ? from : id, to);
	}

	private SqlGenerator sql(Class<?> domainType) {
		return sqlGeneratorSource.getSqlGenerator(domainType);
	}
}
