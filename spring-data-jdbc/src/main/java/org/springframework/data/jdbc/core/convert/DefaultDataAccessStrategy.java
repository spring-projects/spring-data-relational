/*
 * Copyright 2017-2023 the original author or authors.
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.conversion.IdValueSource;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.AggregatePath.TableInfo;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.sql.LockMode;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * The default {@link DataAccessStrategy} is to generate SQL statements based on metadata from the entity.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Thomas Lang
 * @author Bastian Wilhelm
 * @author Christoph Strobl
 * @author Tom Hombergs
 * @author Tyler Van Gorder
 * @author Milan Milanov
 * @author Myeonghyeon Lee
 * @author Yunyoung LEE
 * @author Radim Tlusty
 * @author Chirag Tailor
 * @author Diego Krupitza
 * @since 1.1
 */
public class DefaultDataAccessStrategy implements DataAccessStrategy {

	private final SqlGeneratorSource sqlGeneratorSource;
	private final RelationalMappingContext context;
	private final JdbcConverter converter;
	private final NamedParameterJdbcOperations operations;
	private final SqlParametersFactory sqlParametersFactory;
	private final InsertStrategyFactory insertStrategyFactory;

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
			JdbcConverter converter, NamedParameterJdbcOperations operations, SqlParametersFactory sqlParametersFactory,
			InsertStrategyFactory insertStrategyFactory) {

		Assert.notNull(sqlGeneratorSource, "SqlGeneratorSource must not be null");
		Assert.notNull(context, "RelationalMappingContext must not be null");
		Assert.notNull(converter, "JdbcConverter must not be null");
		Assert.notNull(operations, "NamedParameterJdbcOperations must not be null");
		Assert.notNull(sqlParametersFactory, "SqlParametersFactory must not be null");
		Assert.notNull(insertStrategyFactory, "InsertStrategyFactory must not be null");

		this.sqlGeneratorSource = sqlGeneratorSource;
		this.context = context;
		this.converter = converter;
		this.operations = operations;
		this.sqlParametersFactory = sqlParametersFactory;
		this.insertStrategyFactory = insertStrategyFactory;
	}

	@Override
	public <T> Object insert(T instance, Class<T> domainType, Identifier identifier, IdValueSource idValueSource) {

		SqlIdentifierParameterSource parameterSource = sqlParametersFactory.forInsert(instance, domainType, identifier,
				idValueSource);

		String insertSql = sql(domainType).getInsert(parameterSource.getIdentifiers());

		return insertStrategyFactory.insertStrategy(idValueSource, getIdColumn(domainType)).execute(insertSql,
				parameterSource);
	}

	@Override
	public <T> Object[] insert(List<InsertSubject<T>> insertSubjects, Class<T> domainType, IdValueSource idValueSource) {

		Assert.notEmpty(insertSubjects, "Batch insert must contain at least one InsertSubject");
		SqlIdentifierParameterSource[] sqlParameterSources = insertSubjects.stream()
				.map(insertSubject -> sqlParametersFactory.forInsert(insertSubject.getInstance(), domainType,
						insertSubject.getIdentifier(), idValueSource))
				.toArray(SqlIdentifierParameterSource[]::new);

		String insertSql = sql(domainType).getInsert(sqlParameterSources[0].getIdentifiers());

		return insertStrategyFactory.batchInsertStrategy(idValueSource, getIdColumn(domainType)).execute(insertSql,
				sqlParameterSources);
	}

	@Override
	public <S> boolean update(S instance, Class<S> domainType) {

		SqlIdentifierParameterSource parameterSource = sqlParametersFactory.forUpdate(instance, domainType);
		if (parameterSource.size() <= 1) {
			return true; // returning true, because conceptually the one row was correctly updated
		}
		return operations.update(sql(domainType).getUpdate(), parameterSource) != 0;
	}

	@Override
	public <S> boolean updateWithVersion(S instance, Class<S> domainType, Number previousVersion) {

		RelationalPersistentEntity<S> persistentEntity = getRequiredPersistentEntity(domainType);

		// Adjust update statement to set the new version and use the old version in where clause.
		SqlIdentifierParameterSource parameterSource = sqlParametersFactory.forUpdate(instance, domainType);
		parameterSource.addValue(VERSION_SQL_PARAMETER, previousVersion);

		int affectedRows = operations.update(sql(domainType).getUpdateWithVersion(), parameterSource);

		if (affectedRows == 0) {

			throw new OptimisticLockingFailureException(
					String.format("Optimistic lock exception on saving entity of type %s", persistentEntity.getName()));
		}

		return true;
	}

	@Override
	public void delete(Object id, Class<?> domainType) {

		String deleteByIdSql = sql(domainType).getDeleteById();
		SqlParameterSource parameter = sqlParametersFactory.forQueryById(id, domainType, ID_SQL_PARAMETER);

		operations.update(deleteByIdSql, parameter);
	}

	@Override
	public void delete(Iterable<Object> ids, Class<?> domainType) {

		String deleteByIdInSql = sql(domainType).getDeleteByIdIn();
		SqlParameterSource parameter = sqlParametersFactory.forQueryByIds(ids, domainType);

		operations.update(deleteByIdInSql, parameter);
	}

	@Override
	public <T> void deleteWithVersion(Object id, Class<T> domainType, Number previousVersion) {

		Assert.notNull(id, "Id must not be null");

		RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(domainType);

		SqlIdentifierParameterSource parameterSource = sqlParametersFactory.forQueryById(id, domainType, ID_SQL_PARAMETER);
		parameterSource.addValue(VERSION_SQL_PARAMETER, previousVersion);
		int affectedRows = operations.update(sql(domainType).getDeleteByIdAndVersion(), parameterSource);

		if (affectedRows == 0) {
			throw new OptimisticLockingFailureException(
					String.format("Optimistic lock exception deleting entity of type %s", persistentEntity.getName()));
		}
	}

	@Override
	public void delete(Object rootId, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {

		RelationalPersistentEntity<?> rootEntity = context.getRequiredPersistentEntity(getBaseType(propertyPath));

		RelationalPersistentProperty referencingProperty = propertyPath.getLeafProperty();
		Assert.notNull(referencingProperty, "No property found matching the PropertyPath " + propertyPath);

		String delete = sql(rootEntity.getType()).createDeleteByPath(propertyPath);

		SqlIdentifierParameterSource parameters = sqlParametersFactory.forQueryById(rootId, rootEntity.getType(),
				ROOT_ID_PARAMETER);
		operations.update(delete, parameters);
	}

	@Override
	public void delete(Iterable<Object> rootIds, PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {

		RelationalPersistentEntity<?> rootEntity = context.getRequiredPersistentEntity(getBaseType(propertyPath));

		RelationalPersistentProperty referencingProperty = propertyPath.getLeafProperty();

		Assert.notNull(referencingProperty, "No property found matching the PropertyPath " + propertyPath);

		String delete = sql(rootEntity.getType()).createDeleteInByPath(propertyPath);

		SqlIdentifierParameterSource parameters = sqlParametersFactory.forQueryByIds(rootIds, rootEntity.getType());
		operations.update(delete, parameters);
	}

	@Override
	public <T> void deleteAll(Class<T> domainType) {
		operations.getJdbcOperations().update(sql(domainType).createDeleteAllSql(null));
	}

	@Override
	public void deleteAll(PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {

		operations.getJdbcOperations().update(sql(getBaseType(propertyPath)).createDeleteAllSql(propertyPath));
	}

	@Override
	public <T> void acquireLockById(Object id, LockMode lockMode, Class<T> domainType) {

		String acquireLockByIdSql = sql(domainType).getAcquireLockById(lockMode);
		SqlIdentifierParameterSource parameter = sqlParametersFactory.forQueryById(id, domainType, ID_SQL_PARAMETER);

		operations.query(acquireLockByIdSql, parameter, ResultSet::next);
	}

	@Override
	public <T> void acquireLockAll(LockMode lockMode, Class<T> domainType) {

		String acquireLockAllSql = sql(domainType).getAcquireLockAll(lockMode);
		operations.getJdbcOperations().query(acquireLockAllSql, ResultSet::next);
	}

	@Override
	public long count(Class<?> domainType) {

		Long result = operations.getJdbcOperations().queryForObject(sql(domainType).getCount(), Long.class);

		Assert.notNull(result, "The result of a count query must not be null");

		return result;
	}

	@Override
	public <T> T findById(Object id, Class<T> domainType) {

		String findOneSql = sql(domainType).getFindOne();
		SqlIdentifierParameterSource parameter = sqlParametersFactory.forQueryById(id, domainType, ID_SQL_PARAMETER);

		try {
			return operations.queryForObject(findOneSql, parameter, getEntityRowMapper(domainType));
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {
		return operations.query(sql(domainType).getFindAll(), getEntityRowMapper(domainType));
	}

	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {

		if (!ids.iterator().hasNext()) {
			return Collections.emptyList();
		}

		SqlParameterSource parameterSource = sqlParametersFactory.forQueryByIds(ids, domainType);
		String findAllInListSql = sql(domainType).getFindAllInList();
		return operations.query(findAllInListSql, parameterSource, getEntityRowMapper(domainType));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Iterable<Object> findAllByPath(Identifier identifier,
			PersistentPropertyPath<? extends RelationalPersistentProperty> propertyPath) {

		Assert.notNull(identifier, "identifier must not be null");
		Assert.notNull(propertyPath, "propertyPath must not be null");

		AggregatePath path = context.getAggregatePath(propertyPath);
		Class<?> actualType = path.getLeafEntity().getType();

		String findAllByProperty = sql(actualType) //
				.getFindAllByProperty(identifier, propertyPath);

		SqlParameterSource parameterSource = sqlParametersFactory.forQueryByIdentifier(identifier);
		return operations.query(findAllByProperty, parameterSource, new RowMapper<>() {

			@Override
			public Object mapRow(ResultSet rs, int rowNum) throws SQLException {

				if (path.isMap()) {
					return getMapEntityRowMapper(path, identifier).mapRow(rs, rowNum);
				}

				// Add row number as key for paths that do not define an identifier and that are contained in a collection.
				Identifier identifierToUse = identifier;
				if (!path.hasIdProperty() && path.isQualified()) {

					TableInfo tableInfo = path.getTableInfo();
					identifierToUse = identifierToUse.withPart(tableInfo.qualifierColumnInfo().name(), rowNum, Object.class);
				}

				return getEntityRowMapper(path, identifierToUse).mapRow(rs, rowNum);
			}
		});
	}

	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {

		String existsSql = sql(domainType).getExists();
		SqlParameterSource parameter = sqlParametersFactory.forQueryById(id, domainType, ID_SQL_PARAMETER);

		Boolean result = operations.queryForObject(existsSql, parameter, Boolean.class);
		Assert.state(result != null, "The result of an exists query must not be null");

		return result;
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Sort sort) {
		return operations.query(sql(domainType).getFindAll(sort), getEntityRowMapper(domainType));
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Pageable pageable) {
		return operations.query(sql(domainType).getFindAll(pageable), getEntityRowMapper(domainType));
	}

	@Override
	public <T> Optional<T> findOne(Query query, Class<T> domainType) {

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		String sqlQuery = sql(domainType).selectByQuery(query, parameterSource);

		try {
			return Optional.ofNullable(operations.queryForObject(sqlQuery, parameterSource, getEntityRowMapper(domainType)));
		} catch (EmptyResultDataAccessException e) {
			return Optional.empty();
		}
	}

	@Override
	public <T> Iterable<T> findAll(Query query, Class<T> domainType) {

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		String sqlQuery = sql(domainType).selectByQuery(query, parameterSource);

		return operations.query(sqlQuery, parameterSource, getEntityRowMapper(domainType));
	}

	@Override
	public <T> Iterable<T> findAll(Query query, Class<T> domainType, Pageable pageable) {

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		String sqlQuery = sql(domainType).selectByQuery(query, parameterSource, pageable);

		return operations.query(sqlQuery, parameterSource, getEntityRowMapper(domainType));
	}

	@Override
	public <T> boolean exists(Query query, Class<T> domainType) {

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		String sqlQuery = sql(domainType).existsByQuery(query, parameterSource);

		Boolean result = operations.queryForObject(sqlQuery, parameterSource, Boolean.class);

		Assert.state(result != null, "The result of an exists query must not be null");

		return result;
	}

	@Override
	public <T> long count(Query query, Class<T> domainType) {

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		String sqlQuery = sql(domainType).countByQuery(query, parameterSource);

		Long result = operations.queryForObject(sqlQuery, parameterSource, Long.class);

		Assert.state(result != null, "The result of a count query must not be null.");

		return result;
	}

	private <T> EntityRowMapper<T> getEntityRowMapper(Class<T> domainType) {
		return new EntityRowMapper<>(getRequiredPersistentEntity(domainType), converter);
	}

	private EntityRowMapper<?> getEntityRowMapper(AggregatePath path, Identifier identifier) {
		return new EntityRowMapper<>(path, converter, identifier);
	}

	private RowMapper<?> getMapEntityRowMapper(AggregatePath path, Identifier identifier) {

		AggregatePath.ColumnInfo qualifierColumnInfo = path.getTableInfo().qualifierColumnInfo();
		Assert.notNull(qualifierColumnInfo, () -> "Qualifier column must not be null for " + path);
		SqlIdentifier keyColumn = qualifierColumnInfo.name();

		return new MapEntityRowMapper<>(path, converter, identifier, keyColumn);
	}

	@SuppressWarnings("unchecked")
	private <S> RelationalPersistentEntity<S> getRequiredPersistentEntity(Class<S> domainType) {
		return (RelationalPersistentEntity<S>) context.getRequiredPersistentEntity(domainType);
	}

	private SqlGenerator sql(Class<?> domainType) {
		return sqlGeneratorSource.getSqlGenerator(domainType);
	}

	@Nullable
	private <T> SqlIdentifier getIdColumn(Class<T> domainType) {

		return Optional.ofNullable(context.getRequiredPersistentEntity(domainType).getIdProperty())
				.map(RelationalPersistentProperty::getColumnName).orElse(null);
	}

	private Class<?> getBaseType(PersistentPropertyPath<RelationalPersistentProperty> propertyPath) {

		RelationalPersistentProperty baseProperty = propertyPath.getBaseProperty();

		Assert.notNull(baseProperty, "The base property must not be null");

		return baseProperty.getOwner().getType();
	}

}
