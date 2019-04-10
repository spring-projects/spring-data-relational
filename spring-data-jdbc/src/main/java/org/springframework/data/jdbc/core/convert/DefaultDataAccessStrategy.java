/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import lombok.NonNull;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.jdbc.core.DataAccessStrategy;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.Identifier;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * The default {@link DataAccessStrategy} is to generate SQL statements based on meta data from the entity.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Thomas Lang
 * @author Bastian Wilhelm
 */
public class DefaultDataAccessStrategy implements DataAccessStrategy {

	private final @NonNull SqlGeneratorSource sqlGeneratorSource;
	private final @NonNull RelationalMappingContext context;
	private final @NonNull JdbcConverter converter;
	private final @NonNull NamedParameterJdbcOperations operations;
	private final @NonNull DataAccessStrategy accessStrategy;

	/**
	 * Creates a {@link DefaultDataAccessStrategy} which references it self for resolution of recursive data accesses.
	 * Only suitable if this is the only access strategy in use.
	 *
	 * @param sqlGeneratorSource must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public DefaultDataAccessStrategy(SqlGeneratorSource sqlGeneratorSource, RelationalMappingContext context,
			JdbcConverter converter, NamedParameterJdbcOperations operations) {
		this(sqlGeneratorSource, context, converter, operations, null);
	}

	/**
	 * Creates a {@link DefaultDataAccessStrategy}
	 *
	 * @param sqlGeneratorSource must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @param mappingAccessStrategy can be {@literal null}.
	 * @since 1.1
	 */
	public DefaultDataAccessStrategy(SqlGeneratorSource sqlGeneratorSource, RelationalMappingContext context,
			JdbcConverter converter, NamedParameterJdbcOperations operations,
			@Nullable DataAccessStrategy mappingAccessStrategy) {

		Assert.notNull(sqlGeneratorSource, "SqlGeneratorSource must not be null");
		Assert.notNull(context, "RelationalMappingContext must not be null");
		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(operations, "NamedParameterJdbcOperations must not be null");

		this.sqlGeneratorSource = sqlGeneratorSource;
		this.context = context;
		this.converter = converter;
		this.operations = operations;
		this.accessStrategy = mappingAccessStrategy == null ? this : mappingAccessStrategy;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#insert(java.lang.Object, java.lang.Class, java.util.Map)
	 */
	@Override
	public <T> Object insert(T instance, Class<T> domainType, Map<String, Object> additionalParameters) {
		return insert(instance, domainType, Identifier.from(additionalParameters));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#insert(java.lang.Object, java.lang.Class, java.util.Map)
	 */
	@Override
	public <T> Object insert(T instance, Class<T> domainType, Identifier identifier) {

		KeyHolder holder = new GeneratedKeyHolder();
		RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(domainType);

		MapSqlParameterSource parameterSource = getParameterSource(instance, persistentEntity, "",
				PersistentProperty::isIdProperty);

		identifier.forEach((name, value, type) -> addConvertedPropertyValue(parameterSource, name, value, type));

		Object idValue = getIdValueOrNull(instance, persistentEntity);
		if (idValue != null) {

			RelationalPersistentProperty idProperty = persistentEntity.getRequiredIdProperty();
			addConvertedPropertyValue(parameterSource, idProperty, idValue, idProperty.getColumnName());
		}

		operations.update( //
				sql(domainType).getInsert(new HashSet<>(Arrays.asList(parameterSource.getParameterNames()))), //
				parameterSource, //
				holder //
		);

		return getIdFromHolder(holder, persistentEntity);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#update(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <S> boolean update(S instance, Class<S> domainType) {

		RelationalPersistentEntity<S> persistentEntity = getRequiredPersistentEntity(domainType);

		return operations.update(sql(domainType).getUpdate(),
				getParameterSource(instance, persistentEntity, "", Predicates.includeAll())) != 0;
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
	public void delete(Object rootId, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {

		RelationalPersistentEntity<?> rootEntity = context
				.getRequiredPersistentEntity(propertyPath.getBaseProperty().getOwner().getType());

		RelationalPersistentProperty referencingProperty = propertyPath.getLeafProperty();
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
	public void deleteAll(PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {
		operations.getJdbcOperations()
				.update(sql(propertyPath.getBaseProperty().getOwner().getType()).createDeleteAllSql(propertyPath));
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

		RelationalPersistentProperty idProperty = getRequiredPersistentEntity(domainType).getRequiredIdProperty();
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();

		addConvertedPropertyValuesAsList(parameterSource, idProperty, ids, "ids");

		String findAllInListSql = sql(domainType).getFindAllInList();

		return operations.query(findAllInListSql, parameterSource, (RowMapper<T>) getEntityRowMapper(domainType));
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

	private <S, T> MapSqlParameterSource getParameterSource(S instance, RelationalPersistentEntity<S> persistentEntity,
			String prefix, Predicate<RelationalPersistentProperty> skipProperty) {

		MapSqlParameterSource parameters = new MapSqlParameterSource();

		PersistentPropertyAccessor<S> propertyAccessor = persistentEntity.getPropertyAccessor(instance);

		persistentEntity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) property -> {

			if (skipProperty.test(property)) {
				return;
			}
			if (property.isEntity() && !property.isEmbedded()) {
				return;
			}

			if (property.isEmbedded()) {

				Object value = propertyAccessor.getProperty(property);
				RelationalPersistentEntity<?> embeddedEntity = context.getPersistentEntity(property.getType());
				MapSqlParameterSource additionalParameters = getParameterSource((T) value,
						(RelationalPersistentEntity<T>) embeddedEntity, prefix + property.getEmbeddedPrefix(), skipProperty);
				parameters.addValues(additionalParameters.getValues());
			} else {

				Object value = propertyAccessor.getProperty(property);
				String paramName = prefix + property.getColumnName();

				addConvertedPropertyValue(parameters, property, value, paramName);
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

	@Nullable
	private <S> Object getIdFromHolder(KeyHolder holder, RelationalPersistentEntity<S> persistentEntity) {

		try {
			// MySQL just returns one value with a special name
			return holder.getKey();
		} catch (DataRetrievalFailureException | InvalidDataAccessApiUsageException e) {
			// Postgres returns a value for each column
			// MS SQL Server returns a value that might be null.

			Map<String, Object> keys = holder.getKeys();

			if (keys == null || persistentEntity.getIdProperty() == null) {
				return null;
			}

			return keys.get(persistentEntity.getIdColumn());
		}
	}

	private EntityRowMapper<?> getEntityRowMapper(Class<?> domainType) {
		return new EntityRowMapper<>(getRequiredPersistentEntity(domainType), converter, accessStrategy);
	}

	private RowMapper<?> getMapEntityRowMapper(RelationalPersistentProperty property) {

		String keyColumn = property.getKeyColumn();
		Assert.notNull(keyColumn, () -> "KeyColumn must not be null for " + property);

		return new MapEntityRowMapper<>(getEntityRowMapper(property.getActualType()), keyColumn);
	}

	private <T> MapSqlParameterSource createIdParameterSource(Object id, Class<T> domainType) {

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();

		addConvertedPropertyValue( //
				parameterSource, //
				getRequiredPersistentEntity(domainType).getRequiredIdProperty(), //
				id, //
				"id" //
		);
		return parameterSource;
	}

	private void addConvertedPropertyValue(MapSqlParameterSource parameterSource, RelationalPersistentProperty property,
			Object value, String paramName) {

		JdbcValue jdbcValue = converter.writeJdbcValue( //
				value, //
				property.getColumnType(), //
				property.getSqlType() //
		);

		parameterSource.addValue(paramName, jdbcValue.getValue(), JdbcUtil.sqlTypeFor(jdbcValue.getJdbcType()));
	}

	private void addConvertedPropertyValue(MapSqlParameterSource parameterSource, String name, Object value,
			Class<?> type) {

		JdbcValue jdbcValue = converter.writeJdbcValue( //
				value, //
				type, //
				JdbcUtil.sqlTypeFor(type) //
		);

		parameterSource.addValue( //
				name, //
				jdbcValue.getValue(), //
				JdbcUtil.sqlTypeFor(jdbcValue.getJdbcType()) //
		);
	}

	private void addConvertedPropertyValuesAsList(MapSqlParameterSource parameterSource,
			RelationalPersistentProperty property, Iterable<?> values, String paramName) {

		List<Object> convertedIds = new ArrayList<>();
		JdbcValue jdbcValue = null;
		for (Object id : values) {

			Class<?> columnType = property.getColumnType();
			int sqlType = property.getSqlType();

			jdbcValue = converter.writeJdbcValue(id, columnType, sqlType);
			convertedIds.add(jdbcValue.getValue());
		}

		Assert.notNull(jdbcValue, "JdbcValue must be not null at this point. Please report this as a bug.");

		JDBCType jdbcType = jdbcValue.getJdbcType();
		int typeNumber = jdbcType == null ? JdbcUtils.TYPE_UNKNOWN : jdbcType.getVendorTypeNumber();

		parameterSource.addValue(paramName, convertedIds, typeNumber);
	}

	@SuppressWarnings("unchecked")
	private <S> RelationalPersistentEntity<S> getRequiredPersistentEntity(Class<S> domainType) {
		return (RelationalPersistentEntity<S>) context.getRequiredPersistentEntity(domainType);
	}

	private SqlGenerator sql(Class<?> domainType) {
		return sqlGeneratorSource.getSqlGenerator(domainType);
	}

	/**
	 * Utility to create {@link Predicate}s.
	 */
	static class Predicates {

		/**
		 * Include all {@link Predicate} returning {@literal false} to never skip a property.
		 *
		 * @return the include all {@link Predicate}.
		 */
		static Predicate<RelationalPersistentProperty> includeAll() {
			return it -> false;
		}
	}
}
