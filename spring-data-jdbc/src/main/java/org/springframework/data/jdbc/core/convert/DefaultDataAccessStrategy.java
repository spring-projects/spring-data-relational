/*
 * Copyright 2017-2020 the original author or authors.
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

import static org.springframework.data.jdbc.core.convert.SqlGenerator.*;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.support.JdbcUtil;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
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
 * @author Christoph Strobl
 * @author Tom Hombergs
 * @author Tyler Van Gorder
 * @author Milan Milanov
 * @since 1.1
 */
public class DefaultDataAccessStrategy implements DataAccessStrategy {

	private final SqlGeneratorSource sqlGeneratorSource;
	private final RelationalMappingContext context;
	private final JdbcConverter converter;
	private final NamedParameterJdbcOperations operations;

	/**
	 * Creates a {@link DefaultDataAccessStrategy}
	 *
	 * @param sqlGeneratorSource must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 * @since 1.1
	 */
	public DefaultDataAccessStrategy(SqlGeneratorSource sqlGeneratorSource, RelationalMappingContext context,
			JdbcConverter converter, NamedParameterJdbcOperations operations) {

		Assert.notNull(sqlGeneratorSource, "SqlGeneratorSource must not be null");
		Assert.notNull(context, "RelationalMappingContext must not be null");
		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(operations, "NamedParameterJdbcOperations must not be null");

		this.sqlGeneratorSource = sqlGeneratorSource;
		this.context = context;
		this.converter = converter;
		this.operations = operations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#insert(java.lang.Object, java.lang.Class, java.util.Map)
	 */
	@Override
	public <T> Object insert(T instance, Class<T> domainType, Identifier identifier) {

		SqlGenerator sqlGenerator = sql(domainType);
		RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(domainType);

		SqlIdentifierParameterSource parameterSource = getParameterSource(instance, persistentEntity, "",
				PersistentProperty::isIdProperty, getIdentifierProcessing());

		identifier.forEach((name, value, type) -> addConvertedPropertyValue(parameterSource, name, value, type));

		Object idValue = getIdValueOrNull(instance, persistentEntity);
		if (idValue != null) {

			RelationalPersistentProperty idProperty = persistentEntity.getRequiredIdProperty();
			addConvertedPropertyValue(parameterSource, idProperty, idValue, idProperty.getColumnName());
		}

		KeyHolder holder = new GeneratedKeyHolder();

		operations.update( //
				sqlGenerator.getInsert(new HashSet<>(parameterSource.getIdentifiers())), //
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
				getParameterSource(instance, persistentEntity, "", Predicates.includeAll(), getIdentifierProcessing())) != 0;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#updateWithVersion(java.lang.Object, java.lang.Class, java.lang.Number)
	 */
	@Override
	public <S> boolean updateWithVersion(S instance, Class<S> domainType, Number previousVersion) {

		RelationalPersistentEntity<S> persistentEntity = getRequiredPersistentEntity(domainType);

		// Adjust update statement to set the new version and use the old version in where clause.
		SqlIdentifierParameterSource parameterSource = getParameterSource(instance, persistentEntity, "",
				Predicates.includeAll(), getIdentifierProcessing());
		parameterSource.addValue(VERSION_SQL_PARAMETER, previousVersion);

		int affectedRows = operations.update(sql(domainType).getUpdateWithVersion(), parameterSource);

		if (affectedRows == 0) {

			throw new OptimisticLockingFailureException(
					String.format("Optimistic lock exception on saving entity of type %s.", persistentEntity.getName()));
		}

		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#delete(java.lang.Object, java.lang.Class)
	 */
	@Override
	public void delete(Object id, Class<?> domainType) {

		String deleteByIdSql = sql(domainType).getDeleteById();
		SqlParameterSource parameter = createIdParameterSource(id, domainType);

		operations.update(deleteByIdSql, parameter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#deleteInstance(java.lang.Object, java.lang.Class, java.lang.Number)
	 */
	@Override
	public <T> void deleteWithVersion(Object id, Class<T> domainType, Number previousVersion) {

		Assert.notNull(id, "Id must not be null.");

		RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(domainType);

		SqlIdentifierParameterSource parameterSource = createIdParameterSource(id, domainType);
		parameterSource.addValue(VERSION_SQL_PARAMETER, previousVersion);
		int affectedRows = operations.update(sql(domainType).getDeleteByIdAndVersion(), parameterSource);

		if (affectedRows == 0) {
			throw new OptimisticLockingFailureException(
					String.format("Optimistic lock exception deleting entity of type %s.", persistentEntity.getName()));
		}
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

		String delete = sql(rootEntity.getType()).createDeleteByPath(propertyPath);

		SqlIdentifierParameterSource parameters = new SqlIdentifierParameterSource(getIdentifierProcessing());
		parameters.addValue(ROOT_ID_PARAMETER, rootId);
		operations.update(delete, parameters);
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
	@Override
	@SuppressWarnings("unchecked")
	public <T> T findById(Object id, Class<T> domainType) {

		String findOneSql = sql(domainType).getFindOne();
		SqlIdentifierParameterSource parameter = createIdParameterSource(id, domainType);

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
	@Override
	@SuppressWarnings("unchecked")
	public <T> Iterable<T> findAll(Class<T> domainType) {
		return operations.query(sql(domainType).getFindAll(), (RowMapper<T>) getEntityRowMapper(domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#findAllById(java.lang.Iterable, java.lang.Class)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {

		if (!ids.iterator().hasNext()) {
			return Collections.emptyList();
		}

		RelationalPersistentProperty idProperty = getRequiredPersistentEntity(domainType).getRequiredIdProperty();
		SqlIdentifierParameterSource parameterSource = new SqlIdentifierParameterSource(getIdentifierProcessing());

		addConvertedPropertyValuesAsList(parameterSource, idProperty, ids, IDS_SQL_PARAMETER);

		String findAllInListSql = sql(domainType).getFindAllInList();

		return operations.query(findAllInListSql, parameterSource, (RowMapper<T>) getEntityRowMapper(domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.RelationResolver#findAllByPath(org.springframework.data.jdbc.support.Identifier, org.springframework.data.mapping.PersistentPropertyPath)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Iterable<Object> findAllByPath(Identifier identifier,
			PersistentPropertyPath<? extends RelationalPersistentProperty> propertyPath) {

		Assert.notNull(identifier, "identifier must not be null.");
		Assert.notNull(propertyPath, "propertyPath must not be null.");

		PersistentPropertyPathExtension path = new PersistentPropertyPathExtension(context, propertyPath);

		Class<?> actualType = path.getActualType();
		String findAllByProperty = sql(actualType) //
				.getFindAllByProperty(identifier, path.getQualifierColumn(), path.isOrdered());

		RowMapper<?> rowMapper = path.isMap() ? this.getMapEntityRowMapper(path, identifier)
				: this.getEntityRowMapper(path, identifier);

		return operations.query(findAllByProperty, createParameterSource(identifier, getIdentifierProcessing()),
				(RowMapper<Object>) rowMapper);
	}

	private SqlParameterSource createParameterSource(Identifier identifier, IdentifierProcessing identifierProcessing) {

		SqlIdentifierParameterSource parameterSource = new SqlIdentifierParameterSource(identifierProcessing);

		identifier.toMap().forEach(parameterSource::addValue);

		return parameterSource;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.DataAccessStrategy#existsById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {

		String existsSql = sql(domainType).getExists();
		SqlParameterSource parameter = createIdParameterSource(id, domainType);

		Boolean result = operations.queryForObject(existsSql, parameter, Boolean.class);
		Assert.state(result != null, "The result of an exists query must not be null");

		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.Class, org.springframework.data.domain.Sort)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> Iterable<T> findAll(Class<T> domainType, Sort sort) {
		return operations.query(sql(domainType).getFindAll(sort), (RowMapper<T>) getEntityRowMapper(domainType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.Class, org.springframework.data.domain.Pageable)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> Iterable<T> findAll(Class<T> domainType, Pageable pageable) {
		return operations.query(sql(domainType).getFindAll(pageable), (RowMapper<T>) getEntityRowMapper(domainType));
	}

	private <S, T> SqlIdentifierParameterSource getParameterSource(@Nullable S instance,
			RelationalPersistentEntity<S> persistentEntity, String prefix,
			Predicate<RelationalPersistentProperty> skipProperty, IdentifierProcessing identifierProcessing) {

		SqlIdentifierParameterSource parameters = new SqlIdentifierParameterSource(identifierProcessing);

		PersistentPropertyAccessor<S> propertyAccessor = instance != null ? persistentEntity.getPropertyAccessor(instance)
				: NoValuePropertyAccessor.instance();

		persistentEntity.doWithProperties((PropertyHandler<RelationalPersistentProperty>) property -> {

			if (skipProperty.test(property) || !property.isWritable()) {
				return;
			}
			if (property.isEntity() && !property.isEmbedded()) {
				return;
			}

			if (property.isEmbedded()) {

				Object value = propertyAccessor.getProperty(property);
				RelationalPersistentEntity<?> embeddedEntity = context.getPersistentEntity(property.getType());
				SqlIdentifierParameterSource additionalParameters = getParameterSource((T) value,
						(RelationalPersistentEntity<T>) embeddedEntity, prefix + property.getEmbeddedPrefix(), skipProperty,
						identifierProcessing);
				parameters.addAll(additionalParameters);
			} else {

				Object value = propertyAccessor.getProperty(property);
				SqlIdentifier paramName = property.getColumnName().transform(prefix::concat);

				addConvertedPropertyValue(parameters, property, value, paramName);
			}
		});

		return parameters;
	}

	@Nullable
	@SuppressWarnings("unchecked")
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

			return keys.get(persistentEntity.getIdColumn().getReference(getIdentifierProcessing()));
		}
	}

	private EntityRowMapper<?> getEntityRowMapper(Class<?> domainType) {
		return new EntityRowMapper<>(getRequiredPersistentEntity(domainType), converter);
	}

	private EntityRowMapper<?> getEntityRowMapper(PersistentPropertyPathExtension path, Identifier identifier) {
		return new EntityRowMapper<>(path, converter, identifier);
	}

	private RowMapper<?> getMapEntityRowMapper(PersistentPropertyPathExtension path, Identifier identifier) {

		SqlIdentifier keyColumn = path.getQualifierColumn();
		Assert.notNull(keyColumn, () -> "KeyColumn must not be null for " + path);

		return new MapEntityRowMapper<>(path, converter, identifier, keyColumn, getIdentifierProcessing());
	}

	private <T> SqlIdentifierParameterSource createIdParameterSource(Object id, Class<T> domainType) {

		SqlIdentifierParameterSource parameterSource = new SqlIdentifierParameterSource(getIdentifierProcessing());

		addConvertedPropertyValue( //
				parameterSource, //
				getRequiredPersistentEntity(domainType).getRequiredIdProperty(), //
				id, //
				ID_SQL_PARAMETER //
		);
		return parameterSource;
	}

	private IdentifierProcessing getIdentifierProcessing() {
		return sqlGeneratorSource.getDialect().getIdentifierProcessing();
	}

	private void addConvertedPropertyValue(SqlIdentifierParameterSource parameterSource,
			RelationalPersistentProperty property, @Nullable Object value, SqlIdentifier name) {

		addConvertedValue(parameterSource, value, name, converter.getColumnType(property), converter.getSqlType(property));
	}

	private void addConvertedPropertyValue(SqlIdentifierParameterSource parameterSource, SqlIdentifier name, Object value,
			Class<?> javaType) {

		addConvertedValue(parameterSource, value, name, javaType, JdbcUtil.sqlTypeFor(javaType));
	}

	private void addConvertedValue(SqlIdentifierParameterSource parameterSource, @Nullable Object value,
			SqlIdentifier paramName, Class<?> javaType, int sqlType) {

		JdbcValue jdbcValue = converter.writeJdbcValue( //
				value, //
				javaType, //
				sqlType //
		);

		parameterSource.addValue( //
				paramName, //
				jdbcValue.getValue(), //
				JdbcUtil.sqlTypeFor(jdbcValue.getJdbcType()));
	}

	private void addConvertedPropertyValuesAsList(SqlIdentifierParameterSource parameterSource,
			RelationalPersistentProperty property, Iterable<?> values, SqlIdentifier paramName) {

		List<Object> convertedIds = new ArrayList<>();
		JdbcValue jdbcValue = null;
		for (Object id : values) {

			Class<?> columnType = converter.getColumnType(property);
			int sqlType = converter.getSqlType(property);

			jdbcValue = converter.writeJdbcValue(id, columnType, sqlType);
			convertedIds.add(jdbcValue.getValue());
		}

		Assert.state(jdbcValue != null, "JdbcValue must be not null at this point. Please report this as a bug.");

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

	/**
	 * A {@link PersistentPropertyAccessor} implementation always returning null
	 *
	 * @param <T>
	 */
	static class NoValuePropertyAccessor<T> implements PersistentPropertyAccessor<T> {

		private static final NoValuePropertyAccessor INSTANCE = new NoValuePropertyAccessor();

		static <T> NoValuePropertyAccessor<T> instance() {
			return INSTANCE;
		}

		@Override
		public void setProperty(PersistentProperty<?> property, @Nullable Object value) {
			throw new UnsupportedOperationException("Cannot set value on 'null' target object.");
		}

		@Override
		public Object getProperty(PersistentProperty<?> property) {
			return null;
		}

		@Override
		public T getBean() {
			return null;
		}
	}
}
