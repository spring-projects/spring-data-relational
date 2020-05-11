/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.beans.FeatureDescriptor;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.core.convert.ConversionService;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionInformation;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.query.Update;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.util.ProxyUtils;
import org.springframework.util.Assert;

/**
 * Implementation of {@link R2dbcEntityOperations}. It simplifies the use of Reactive R2DBC usage through entities and
 * helps to avoid common errors. This class uses {@link DatabaseClient} to execute SQL queries or updates, initiating
 * iteration over {@link io.r2dbc.spi.Result}.
 * <p>
 * Can be used within a service implementation via direct instantiation with a {@link DatabaseClient} reference, or get
 * prepared in an application context and given to services as bean reference.
 *
 * @author Mark Paluch
 * @author Bogdan Ilchyshyn
 * @since 1.1
 */
public class R2dbcEntityTemplate implements R2dbcEntityOperations, BeanFactoryAware {

	private final DatabaseClient databaseClient;

	private final ReactiveDataAccessStrategy dataAccessStrategy;

	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;

	private final SpelAwareProxyProjectionFactory projectionFactory;

	/**
	 * Create a new {@link R2dbcEntityTemplate} given {@link DatabaseClient}.
	 *
	 * @param databaseClient must not be {@literal null}.
	 */
	public R2dbcEntityTemplate(DatabaseClient databaseClient) {
		this(databaseClient, getDataAccessStrategy(databaseClient));
	}

	/**
	 * Create a new {@link R2dbcEntityTemplate} given {@link DatabaseClient} and {@link ReactiveDataAccessStrategy}.
	 *
	 * @param databaseClient must not be {@literal null}.
	 */
	public R2dbcEntityTemplate(DatabaseClient databaseClient, ReactiveDataAccessStrategy strategy) {

		Assert.notNull(databaseClient, "DatabaseClient must not be null");
		Assert.notNull(strategy, "ReactiveDataAccessStrategy must not be null");

		this.databaseClient = databaseClient;
		this.dataAccessStrategy = strategy;
		this.mappingContext = strategy.getConverter().getMappingContext();
		this.projectionFactory = new SpelAwareProxyProjectionFactory();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.R2dbcEntityOperations#getDatabaseClient()
	 */
	@Override
	public DatabaseClient getDatabaseClient() {
		return this.databaseClient;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.BeanFactoryAware#setBeanFactory(org.springframework.beans.factory.BeanFactory)
	 */
	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.projectionFactory.setBeanFactory(beanFactory);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.r2dbc.core.FluentR2dbcOperations
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.ReactiveSelectOperation#select(java.lang.Class)
	 */
	@Override
	public <T> ReactiveSelect<T> select(Class<T> domainType) {
		return new ReactiveSelectOperationSupport(this).select(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.ReactiveInsertOperation#insert(java.lang.Class)
	 */
	@Override
	public <T> ReactiveInsert<T> insert(Class<T> domainType) {
		return new ReactiveInsertOperationSupport(this).insert(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.ReactiveUpdateOperation#update(java.lang.Class)
	 */
	@Override
	public ReactiveUpdate update(Class<?> domainType) {
		return new ReactiveUpdateOperationSupport(this).update(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.ReactiveDeleteOperation#delete(java.lang.Class)
	 */
	@Override
	public ReactiveDelete delete(Class<?> domainType) {
		return new ReactiveDeleteOperationSupport(this).delete(domainType);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.r2dbc.query.Query
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.R2dbcEntityOperations#count(org.springframework.data.r2dbc.query.Query, java.lang.Class)
	 */
	@Override
	public Mono<Long> count(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "entity class must not be null");

		return doCount(query, entityClass, getTableName(entityClass));
	}

	Mono<Long> doCount(Query query, Class<?> entityClass, SqlIdentifier tableName) {

		RelationalPersistentEntity<?> entity = getRequiredEntity(entityClass);
		StatementMapper statementMapper = dataAccessStrategy.getStatementMapper().forType(entityClass);

		StatementMapper.SelectSpec selectSpec = statementMapper //
				.createSelect(tableName) //
				.doWithTable((table, spec) -> {
					return spec.withProjection(Functions.count(table.column(entity.getRequiredIdProperty().getColumnName())));
				});

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		if (criteria.isPresent()) {
			selectSpec = criteria.map(selectSpec::withCriteria).orElse(selectSpec);
		}

		PreparedOperation<?> operation = statementMapper.getMappedObject(selectSpec);

		return this.databaseClient.execute(operation) //
				.map((r, md) -> r.get(0, Long.class)) //
				.first() //
				.defaultIfEmpty(0L);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.R2dbcEntityOperations#exists(org.springframework.data.r2dbc.query.Query, java.lang.Class)
	 */
	@Override
	public Mono<Boolean> exists(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "entity class must not be null");

		return doExists(query, entityClass, getTableName(entityClass));
	}

	Mono<Boolean> doExists(Query query, Class<?> entityClass, SqlIdentifier tableName) {

		RelationalPersistentEntity<?> entity = getRequiredEntity(entityClass);
		StatementMapper statementMapper = dataAccessStrategy.getStatementMapper().forType(entityClass);

		SqlIdentifier columnName = entity.hasIdProperty() ? entity.getRequiredIdProperty().getColumnName()
				: SqlIdentifier.unquoted("*");

		StatementMapper.SelectSpec selectSpec = statementMapper //
				.createSelect(tableName) //
				.withProjection(columnName) //
				.limit(1);

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		if (criteria.isPresent()) {
			selectSpec = criteria.map(selectSpec::withCriteria).orElse(selectSpec);
		}

		PreparedOperation<?> operation = statementMapper.getMappedObject(selectSpec);

		return this.databaseClient.execute(operation) //
				.map((r, md) -> r) //
				.first() //
				.hasElement();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.R2dbcEntityOperations#select(org.springframework.data.r2dbc.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Flux<T> select(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "entity class must not be null");

		return doSelect(query, entityClass, getTableName(entityClass), entityClass).all();
	}

	<T> RowsFetchSpec<T> doSelect(Query query, Class<?> entityClass, SqlIdentifier tableName, Class<T> returnType) {

		StatementMapper statementMapper = dataAccessStrategy.getStatementMapper().forType(entityClass);

		StatementMapper.SelectSpec selectSpec = statementMapper //
				.createSelect(tableName) //
				.doWithTable((table, spec) -> spec.withProjection(getSelectProjection(table, query, returnType)));

		if (query.getLimit() > 0) {
			selectSpec = selectSpec.limit(query.getLimit());
		}

		if (query.getOffset() > 0) {
			selectSpec = selectSpec.offset(query.getOffset());
		}

		if (query.isSorted()) {
			selectSpec = selectSpec.withSort(query.getSort());
		}

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		if (criteria.isPresent()) {
			selectSpec = criteria.map(selectSpec::withCriteria).orElse(selectSpec);
		}

		PreparedOperation<?> operation = statementMapper.getMappedObject(selectSpec);

		BiFunction<Row, RowMetadata, T> rowMapper;
		if (returnType.isInterface()) {
			rowMapper = dataAccessStrategy.getRowMapper(entityClass)
					.andThen(o -> projectionFactory.createProjection(returnType, o));
		} else {
			rowMapper = dataAccessStrategy.getRowMapper(returnType);
		}

		return this.databaseClient.execute(operation).map(rowMapper);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.R2dbcEntityOperations#selectOne(org.springframework.data.r2dbc.query.Query, java.lang.Class)
	 */
	@Override
	public <T> Mono<T> selectOne(Query query, Class<T> entityClass) throws DataAccessException {
		return doSelect(query.limit(2), entityClass, getTableName(entityClass), entityClass).one();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.R2dbcEntityOperations#update(org.springframework.data.r2dbc.query.Query, org.springframework.data.r2dbc.query.Update, java.lang.Class)
	 */
	@Override
	public Mono<Integer> update(Query query, Update update, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entityClass, "entity class must not be null");

		return doUpdate(query, update, entityClass, getTableName(entityClass));
	}

	Mono<Integer> doUpdate(Query query, Update update, Class<?> entityClass, SqlIdentifier tableName) {

		StatementMapper statementMapper = dataAccessStrategy.getStatementMapper().forType(entityClass);

		StatementMapper.UpdateSpec selectSpec = statementMapper //
				.createUpdate(tableName, update);

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		if (criteria.isPresent()) {
			selectSpec = criteria.map(selectSpec::withCriteria).orElse(selectSpec);
		}

		PreparedOperation<?> operation = statementMapper.getMappedObject(selectSpec);
		return this.databaseClient.execute(operation).fetch().rowsUpdated();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.R2dbcEntityOperations#delete(org.springframework.data.r2dbc.query.Query, java.lang.Class)
	 */
	@Override
	public Mono<Integer> delete(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "entity class must not be null");

		return doDelete(query, entityClass, getTableName(entityClass));
	}

	Mono<Integer> doDelete(Query query, Class<?> entityClass, SqlIdentifier tableName) {

		StatementMapper statementMapper = dataAccessStrategy.getStatementMapper().forType(entityClass);

		StatementMapper.DeleteSpec selectSpec = statementMapper //
				.createDelete(tableName);

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		if (criteria.isPresent()) {
			selectSpec = criteria.map(selectSpec::withCriteria).orElse(selectSpec);
		}

		PreparedOperation<?> operation = statementMapper.getMappedObject(selectSpec);
		return this.databaseClient.execute(operation).fetch().rowsUpdated().defaultIfEmpty(0);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.R2dbcEntityOperations#insert(java.lang.Object)
	 */
	@Override
	public <T> Mono<T> insert(T entity) throws DataAccessException {

		Assert.notNull(entity, "Entity must not be null");

		return doInsert(entity, getRequiredEntity(entity).getTableName());
	}

	<T> Mono<T> doInsert(T entity, SqlIdentifier tableName) {

		RelationalPersistentEntity<T> persistentEntity = getRequiredEntity(entity);

		T entityToInsert = setVersionIfNecessary(persistentEntity, entity);

		return this.databaseClient.insert() //
				.into(persistentEntity.getType()) //
				.table(tableName).using(entityToInsert) //
				.map(this.dataAccessStrategy.getConverter().populateIdIfNecessary(entityToInsert)) //
				.first() //
				.defaultIfEmpty(entityToInsert);
	}

	@SuppressWarnings("unchecked")
	private <T> T setVersionIfNecessary(RelationalPersistentEntity<T> persistentEntity, T entity) {

		RelationalPersistentProperty versionProperty = persistentEntity.getVersionProperty();
		if (versionProperty == null) {
			return entity;
		}

		Class<?> versionPropertyType = versionProperty.getType();
		Long version = versionPropertyType.isPrimitive() ? 1L : 0L;
		ConversionService conversionService = this.dataAccessStrategy.getConverter().getConversionService();
		PersistentPropertyAccessor<?> propertyAccessor = persistentEntity.getPropertyAccessor(entity);
		propertyAccessor.setProperty(versionProperty, conversionService.convert(version, versionPropertyType));

		return (T) propertyAccessor.getBean();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.R2dbcEntityOperations#update(java.lang.Object)
	 */
	@Override
	public <T> Mono<T> update(T entity) throws DataAccessException {

		Assert.notNull(entity, "Entity must not be null");

		RelationalPersistentEntity<T> persistentEntity = getRequiredEntity(entity);

		DatabaseClient.TypedUpdateSpec<T> updateMatchingSpec = this.databaseClient.update() //
				.table(persistentEntity.getType()) //
				.table(persistentEntity.getTableName());

		DatabaseClient.UpdateSpec matching;
		T entityToUpdate;
		if (persistentEntity.hasVersionProperty()) {

			Criteria criteria = createMatchingVersionCriteria(entity, persistentEntity);
			entityToUpdate = incrementVersion(persistentEntity, entity);
			matching = updateMatchingSpec.using(entityToUpdate).matching(criteria);
		} else {
			entityToUpdate = entity;
			matching = updateMatchingSpec.using(entity);
		}

		return matching.fetch() //
				.rowsUpdated() //
				.flatMap(rowsUpdated -> rowsUpdated == 0 ? handleMissingUpdate(entityToUpdate, persistentEntity)
						: Mono.just(entityToUpdate));
	}

	private <T> Mono<? extends T> handleMissingUpdate(T entity, RelationalPersistentEntity<T> persistentEntity) {

		return Mono.error(persistentEntity.hasVersionProperty()
				? new OptimisticLockingFailureException(formatOptimisticLockingExceptionMessage(entity, persistentEntity))
				: new TransientDataAccessResourceException(formatTransientEntityExceptionMessage(entity, persistentEntity)));
	}

	private <T> String formatOptimisticLockingExceptionMessage(T entity, RelationalPersistentEntity<T> persistentEntity) {

		return String.format("Failed to update table [%s]. Version does not match for row with Id [%s].",
				persistentEntity.getTableName(), persistentEntity.getIdentifierAccessor(entity).getIdentifier());
	}

	private <T> String formatTransientEntityExceptionMessage(T entity, RelationalPersistentEntity<T> persistentEntity) {

		return String.format("Failed to update table [%s]. Row with Id [%s] does not exist.",
				persistentEntity.getTableName(), persistentEntity.getIdentifierAccessor(entity).getIdentifier());
	}

	@SuppressWarnings("unchecked")
	private <T> T incrementVersion(RelationalPersistentEntity<T> persistentEntity, T entity) {

		PersistentPropertyAccessor<?> propertyAccessor = persistentEntity.getPropertyAccessor(entity);
		RelationalPersistentProperty versionProperty = persistentEntity.getVersionProperty();

		ConversionService conversionService = this.dataAccessStrategy.getConverter().getConversionService();
		Object currentVersionValue = propertyAccessor.getProperty(versionProperty);
		long newVersionValue = 1L;
		if (currentVersionValue != null) {
			newVersionValue = conversionService.convert(currentVersionValue, Long.class) + 1;
		}
		Class<?> versionPropertyType = versionProperty.getType();
		propertyAccessor.setProperty(versionProperty, conversionService.convert(newVersionValue, versionPropertyType));

		return (T) propertyAccessor.getBean();
	}

	private <T> Criteria createMatchingVersionCriteria(T entity, RelationalPersistentEntity<T> persistentEntity) {

		PersistentPropertyAccessor<?> propertyAccessor = persistentEntity.getPropertyAccessor(entity);
		RelationalPersistentProperty versionProperty = persistentEntity.getVersionProperty();

		Object version = propertyAccessor.getProperty(versionProperty);
		Criteria.CriteriaStep versionColumn = Criteria.where(dataAccessStrategy.toSql(versionProperty.getColumnName()));
		if (version == null) {
			return versionColumn.isNull();
		} else {
			return versionColumn.is(version);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.R2dbcEntityOperations#delete(java.lang.Object)
	 */
	@Override
	public <T> Mono<T> delete(T entity) throws DataAccessException {

		Assert.notNull(entity, "Entity must not be null");

		RelationalPersistentEntity<?> persistentEntity = getRequiredEntity(entity);

		return delete(getByIdQuery(entity, persistentEntity), persistentEntity.getType()).thenReturn(entity);
	}

	private <T> Query getByIdQuery(T entity, RelationalPersistentEntity<?> persistentEntity) {
		if (!persistentEntity.hasIdProperty()) {
			throw new MappingException("No id property found for object of type " + persistentEntity.getType() + "!");
		}

		IdentifierAccessor identifierAccessor = persistentEntity.getIdentifierAccessor(entity);
		Object id = identifierAccessor.getRequiredIdentifier();

		return Query.query(Criteria.where(persistentEntity.getRequiredIdProperty().getName()).is(id));
	}

	SqlIdentifier getTableName(Class<?> entityClass) {
		return getRequiredEntity(entityClass).getTableName();
	}

	private RelationalPersistentEntity<?> getRequiredEntity(Class<?> entityClass) {
		return this.mappingContext.getRequiredPersistentEntity(entityClass);
	}

	private <T> RelationalPersistentEntity<T> getRequiredEntity(T entity) {
		Class<?> entityType = ProxyUtils.getUserClass(entity);
		return (RelationalPersistentEntity) getRequiredEntity(entityType);
	}

	private <T> List<Expression> getSelectProjection(Table table, Query query, Class<T> returnType) {

		if (query.getColumns().isEmpty()) {

			if (returnType.isInterface()) {

				ProjectionInformation projectionInformation = projectionFactory.getProjectionInformation(returnType);

				if (projectionInformation.isClosed()) {
					return projectionInformation.getInputProperties().stream().map(FeatureDescriptor::getName).map(table::column)
							.collect(Collectors.toList());
				}
			}

			return Collections.singletonList(table.asterisk());
		}

		return query.getColumns().stream().map(table::column).collect(Collectors.toList());
	}

	private static ReactiveDataAccessStrategy getDataAccessStrategy(DatabaseClient databaseClient) {

		Assert.notNull(databaseClient, "DatabaseClient must not be null");

		if (databaseClient instanceof DefaultDatabaseClient) {

			DefaultDatabaseClient client = (DefaultDatabaseClient) databaseClient;
			return client.getDataAccessStrategy();
		}

		throw new IllegalStateException("Cannot obtain ReactiveDataAccessStrategy");
	}

}
