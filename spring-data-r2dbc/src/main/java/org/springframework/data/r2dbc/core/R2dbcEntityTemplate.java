/*
 * Copyright 2020-2024 the original author or authors.
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

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.convert.ConversionService;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.callback.ReactiveEntityCallbacks;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.mapping.event.AfterConvertCallback;
import org.springframework.data.r2dbc.mapping.event.AfterSaveCallback;
import org.springframework.data.r2dbc.mapping.event.BeforeConvertCallback;
import org.springframework.data.r2dbc.mapping.event.BeforeSaveCallback;
import org.springframework.data.relational.core.conversion.AbstractRelationalConverter;
import org.springframework.data.relational.core.mapping.PersistentPropertyTranslator;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.query.Update;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Expressions;
import org.springframework.data.relational.core.sql.Functions;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.domain.RowDocument;
import org.springframework.data.util.Predicates;
import org.springframework.data.util.ProxyUtils;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.Parameter;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.RowsFetchSpec;
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
 * @author Jens Schauder
 * @author Jose Luis Leon
 * @author Robert Heim
 * @author Sebastian Wieland
 * @author Mikhail Polivakha
 * @since 1.1
 */
public class R2dbcEntityTemplate implements R2dbcEntityOperations, BeanFactoryAware, ApplicationContextAware {

	private final DatabaseClient databaseClient;

	private final ReactiveDataAccessStrategy dataAccessStrategy;

	private final R2dbcConverter converter;

	private final MappingContext<? extends RelationalPersistentEntity<?>, ? extends RelationalPersistentProperty> mappingContext;

	private final SpelAwareProxyProjectionFactory projectionFactory;

	private @Nullable ReactiveEntityCallbacks entityCallbacks;

	private Function<Statement, Statement> statementFilterFunction = Function.identity();

	/**
	 * Create a new {@link R2dbcEntityTemplate} given {@link ConnectionFactory}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @since 1.2
	 */
	public R2dbcEntityTemplate(ConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");

		R2dbcDialect dialect = DialectResolver.getDialect(connectionFactory);

		this.databaseClient = DatabaseClient.builder().connectionFactory(connectionFactory)
				.bindMarkers(dialect.getBindMarkersFactory()).build();
		this.dataAccessStrategy = new DefaultReactiveDataAccessStrategy(dialect);
		this.converter = dataAccessStrategy.getConverter();
		this.mappingContext = converter.getMappingContext();
		this.projectionFactory = new SpelAwareProxyProjectionFactory();
	}

	/**
	 * Create a new {@link R2dbcEntityTemplate} given {@link DatabaseClient}.
	 *
	 * @param databaseClient must not be {@literal null}.
	 * @param dialect the dialect to use, must not be {@literal null}.
	 * @since 1.2
	 */
	public R2dbcEntityTemplate(DatabaseClient databaseClient, R2dbcDialect dialect) {
		this(databaseClient, new DefaultReactiveDataAccessStrategy(dialect));
	}

	/**
	 * Create a new {@link R2dbcEntityTemplate} given {@link DatabaseClient}, {@link R2dbcDialect} and
	 * {@link R2dbcConverter}.
	 *
	 * @param databaseClient must not be {@literal null}.
	 * @param dialect the dialect to use, must not be {@literal null}.
	 * @param converter the dialect to use, must not be {@literal null}.
	 * @since 1.2
	 */
	public R2dbcEntityTemplate(DatabaseClient databaseClient, R2dbcDialect dialect, R2dbcConverter converter) {
		this(databaseClient, new DefaultReactiveDataAccessStrategy(dialect, converter));
	}

	/**
	 * Create a new {@link R2dbcEntityTemplate} given {@link DatabaseClient} and {@link ReactiveDataAccessStrategy}.
	 *
	 * @param databaseClient must not be {@literal null}.
	 * @since 1.2
	 */
	public R2dbcEntityTemplate(DatabaseClient databaseClient, ReactiveDataAccessStrategy strategy) {

		Assert.notNull(databaseClient, "DatabaseClient must not be null");
		Assert.notNull(strategy, "ReactiveDataAccessStrategy must not be null");

		this.databaseClient = databaseClient;
		this.dataAccessStrategy = strategy;
		this.converter = dataAccessStrategy.getConverter();
		this.mappingContext = strategy.getConverter().getMappingContext();
		this.projectionFactory = new SpelAwareProxyProjectionFactory();
	}

	/**
	 * Set a {@link Function Statement Filter Function} that is applied to every {@link Statement}.
	 *
	 * @param statementFilterFunction must not be {@literal null}.
	 * @since 3.4
	 */
	public void setStatementFilterFunction(Function<Statement, Statement> statementFilterFunction) {

		Assert.notNull(statementFilterFunction, "StatementFilterFunction must not be null");

		this.statementFilterFunction = statementFilterFunction;
	}

	@Override
	public DatabaseClient getDatabaseClient() {
		return this.databaseClient;
	}

	@Override
	public ReactiveDataAccessStrategy getDataAccessStrategy() {
		return this.dataAccessStrategy;
	}

	@Override
	public R2dbcConverter getConverter() {
		return this.converter;
	}

	@Override
	@Deprecated
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

		if (entityCallbacks == null) {
			setEntityCallbacks(ReactiveEntityCallbacks.create(applicationContext));
		}

		projectionFactory.setBeanFactory(applicationContext);
		projectionFactory.setBeanClassLoader(applicationContext.getClassLoader());
	}

	/**
	 * Set the {@link ReactiveEntityCallbacks} instance to use when invoking
	 * {@link org.springframework.data.mapping.callback.ReactiveEntityCallbacks callbacks} like the
	 * {@link BeforeSaveCallback}.
	 * <p>
	 * Overrides potentially existing {@link ReactiveEntityCallbacks}.
	 *
	 * @param entityCallbacks must not be {@literal null}.
	 * @throws IllegalArgumentException if the given instance is {@literal null}.
	 * @since 1.2
	 */
	public void setEntityCallbacks(ReactiveEntityCallbacks entityCallbacks) {

		Assert.notNull(entityCallbacks, "EntityCallbacks must not be null");
		this.entityCallbacks = entityCallbacks;
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.r2dbc.core.FluentR2dbcOperations
	// -------------------------------------------------------------------------

	@Override
	public <T> ReactiveSelect<T> select(Class<T> domainType) {
		return new ReactiveSelectOperationSupport(this).select(domainType);
	}

	@Override
	public <T> ReactiveInsert<T> insert(Class<T> domainType) {
		return new ReactiveInsertOperationSupport(this).insert(domainType);
	}

	@Override
	public ReactiveUpdate update(Class<?> domainType) {
		return new ReactiveUpdateOperationSupport(this).update(domainType);
	}

	@Override
	public ReactiveDelete delete(Class<?> domainType) {
		return new ReactiveDeleteOperationSupport(this).delete(domainType);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.r2dbc.query.Query
	// -------------------------------------------------------------------------

	@Override
	public Mono<Long> count(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity class must not be null");

		return doCount(query, entityClass, getTableName(entityClass));
	}

	Mono<Long> doCount(Query query, Class<?> entityClass, SqlIdentifier tableName) {

		StatementMapper statementMapper = dataAccessStrategy.getStatementMapper().forType(entityClass);

		StatementMapper.SelectSpec selectSpec = statementMapper //
				.createSelect(tableName) //
				.withProjection(Functions.count(Expressions.just("*")));

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		if (criteria.isPresent()) {
			selectSpec = criteria.map(selectSpec::withCriteria).orElse(selectSpec);
		}

		PreparedOperation<?> operation = statementMapper.getMappedObject(selectSpec);

		return this.databaseClient.sql(operation) //
				.filter(statementFilterFunction) //
				.map((r, md) -> r.get(0, Long.class)) //
				.first() //
				.defaultIfEmpty(0L);
	}

	@Override
	public Mono<Boolean> exists(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity class must not be null");

		return doExists(query, entityClass, getTableName(entityClass));
	}

	Mono<Boolean> doExists(Query query, Class<?> entityClass, SqlIdentifier tableName) {

		StatementMapper statementMapper = dataAccessStrategy.getStatementMapper().forType(entityClass);
		StatementMapper.SelectSpec selectSpec = statementMapper.createSelect(tableName).limit(1)
				.withProjection(Expressions.just("1"));

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		if (criteria.isPresent()) {
			selectSpec = criteria.map(selectSpec::withCriteria).orElse(selectSpec);
		}

		PreparedOperation<?> operation = statementMapper.getMappedObject(selectSpec);

		return this.databaseClient.sql(operation) //
				.filter(statementFilterFunction) //
				.map((r, md) -> r) //
				.first() //
				.hasElement();
	}

	@Override
	public <T> Flux<T> select(Query query, Class<T> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity class must not be null");

		SqlIdentifier tableName = getTableName(entityClass);
		return doSelect(query, entityClass, tableName, entityClass, RowsFetchSpec::all, null);
	}

	@SuppressWarnings("unchecked")
	<T, P extends Publisher<T>> P doSelect(Query query, Class<?> entityClass, SqlIdentifier tableName,
			Class<T> returnType, Function<RowsFetchSpec<T>, P> resultHandler, @Nullable Integer fetchSize) {

		RowsFetchSpec<T> fetchSpec = doSelect(query, entityClass, tableName, returnType,
				fetchSize != null ? statement -> statement.fetchSize(fetchSize) : Function.identity());

		P result = resultHandler.apply(fetchSpec);

		if (result instanceof Mono) {
			return (P) ((Mono<?>) result).flatMap(it -> maybeCallAfterConvert(it, tableName));
		}

		return (P) ((Flux<?>) result).concatMap(it -> maybeCallAfterConvert(it, tableName));
	}

	private <T> RowsFetchSpec<T> doSelect(Query query, Class<?> entityType, SqlIdentifier tableName,
			Class<T> returnType, Function<? super Statement, ? extends Statement> filterFunction) {

		StatementMapper statementMapper = dataAccessStrategy.getStatementMapper().forType(entityType);

		StatementMapper.SelectSpec selectSpec = statementMapper //
				.createSelect(tableName) //
				.doWithTable((table, spec) -> spec.withProjection(getSelectProjection(table, query, entityType, returnType)));

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

		return getRowsFetchSpec(
				databaseClient.sql(operation).filter(statementFilterFunction.andThen(filterFunction)),
			entityType,
			returnType
		);
	}

	@Override
	public <T> Mono<T> selectOne(Query query, Class<T> entityClass) throws DataAccessException {
		return doSelect(query.isLimited() ? query : query.limit(2), entityClass, getTableName(entityClass), entityClass,
				RowsFetchSpec::one, null);
	}

	@Override
	public Mono<Long> update(Query query, Update update, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entityClass, "Entity class must not be null");

		return doUpdate(query, update, entityClass, getTableName(entityClass));
	}

	Mono<Long> doUpdate(Query query, Update update, Class<?> entityClass, SqlIdentifier tableName) {

		StatementMapper statementMapper = dataAccessStrategy.getStatementMapper().forType(entityClass);

		StatementMapper.UpdateSpec selectSpec = statementMapper //
				.createUpdate(tableName, update);

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		if (criteria.isPresent()) {
			selectSpec = criteria.map(selectSpec::withCriteria).orElse(selectSpec);
		}

		PreparedOperation<?> operation = statementMapper.getMappedObject(selectSpec);
		return this.databaseClient.sql(operation).filter(statementFilterFunction).fetch().rowsUpdated();
	}

	@Override
	public Mono<Long> delete(Query query, Class<?> entityClass) throws DataAccessException {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entityClass, "Entity class must not be null");

		return doDelete(query, entityClass, getTableName(entityClass));
	}

	Mono<Long> doDelete(Query query, Class<?> entityClass, SqlIdentifier tableName) {

		StatementMapper statementMapper = dataAccessStrategy.getStatementMapper().forType(entityClass);

		StatementMapper.DeleteSpec deleteSpec = statementMapper //
				.createDelete(tableName);

		Optional<CriteriaDefinition> criteria = query.getCriteria();
		if (criteria.isPresent()) {
			deleteSpec = criteria.map(deleteSpec::withCriteria).orElse(deleteSpec);
		}

		PreparedOperation<?> operation = statementMapper.getMappedObject(deleteSpec);
		return this.databaseClient.sql(operation).filter(statementFilterFunction).fetch().rowsUpdated().defaultIfEmpty(0L);
	}

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.r2dbc.core.PreparedOperation
	// -------------------------------------------------------------------------

	@Override
	public <T> RowsFetchSpec<T> query(PreparedOperation<?> operation, Class<T> entityClass) {
		return query(operation, entityClass, entityClass);
	}

	@Override
	public <T> RowsFetchSpec<T> query(PreparedOperation<?> operation, Class<?> entityClass, Class<T> resultType)
			throws DataAccessException {

		Assert.notNull(operation, "PreparedOperation must not be null");
		Assert.notNull(entityClass, "Entity class must not be null");

		return new EntityCallbackAdapter<>(
				getRowsFetchSpec(databaseClient.sql(operation).filter(statementFilterFunction), entityClass, resultType),
				getTableNameOrEmpty(entityClass));
	}

	@Override
	public <T> RowsFetchSpec<T> query(PreparedOperation<?> operation, BiFunction<Row, RowMetadata, T> rowMapper) {

		Assert.notNull(operation, "PreparedOperation must not be null");
		Assert.notNull(rowMapper, "Row mapper must not be null");

		return new EntityCallbackAdapter<>(databaseClient.sql(operation).filter(statementFilterFunction).map(rowMapper),
				SqlIdentifier.EMPTY);
	}

	@Override
	public <T> RowsFetchSpec<T> query(PreparedOperation<?> operation, Class<?> entityClass,
			BiFunction<Row, RowMetadata, T> rowMapper) {

		Assert.notNull(operation, "PreparedOperation must not be null");
		Assert.notNull(entityClass, "Entity class must not be null");
		Assert.notNull(rowMapper, "Row mapper must not be null");

		return new EntityCallbackAdapter<>(databaseClient.sql(operation).filter(statementFilterFunction).map(rowMapper),
				getTableNameOrEmpty(entityClass));
	}

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	@Override
	public <T> Mono<T> insert(T entity) throws DataAccessException {

		Assert.notNull(entity, "Entity must not be null");

		return doInsert(entity, getRequiredEntity(entity).getQualifiedTableName());
	}

	<T> Mono<T> doInsert(T entity, SqlIdentifier tableName) {

		RelationalPersistentEntity<T> persistentEntity = getRequiredEntity(entity);

		return maybeCallBeforeConvert(entity, tableName).flatMap(onBeforeConvert -> {

			T initializedEntity = setVersionIfNecessary(persistentEntity, onBeforeConvert);

			OutboundRow outboundRow = dataAccessStrategy.getOutboundRow(initializedEntity);

			potentiallyRemoveId(persistentEntity, outboundRow);

			return maybeCallBeforeSave(initializedEntity, outboundRow, tableName) //
					.flatMap(entityToSave -> doInsert(entityToSave, tableName, outboundRow));
		});
	}

	private void potentiallyRemoveId(RelationalPersistentEntity<?> persistentEntity, OutboundRow outboundRow) {

		RelationalPersistentProperty idProperty = persistentEntity.getIdProperty();
		if (idProperty == null) {
			return;
		}

		SqlIdentifier columnName = idProperty.getColumnName();
		Parameter parameter = outboundRow.get(columnName);

		if (shouldSkipIdValue(parameter, idProperty)) {
			outboundRow.remove(columnName);
		}
	}

	private boolean shouldSkipIdValue(@Nullable Parameter value, RelationalPersistentProperty property) {

		if (value == null || value.getValue() == null) {
			return true;
		}

		if (value.getValue() instanceof Number) {
			return ((Number) value.getValue()).longValue() == 0L;
		}

		return false;
	}

	private <T> Mono<T> doInsert(T entity, SqlIdentifier tableName, OutboundRow outboundRow) {

		StatementMapper mapper = dataAccessStrategy.getStatementMapper();
		StatementMapper.InsertSpec insert = mapper.createInsert(tableName);

		for (SqlIdentifier column : outboundRow.keySet()) {
			Parameter settableValue = outboundRow.get(column);
			if (settableValue.hasValue()) {
				insert = insert.withColumn(column, settableValue);
			}
		}

		PreparedOperation<?> operation = mapper.getMappedObject(insert);

		List<SqlIdentifier> identifierColumns = dataAccessStrategy.getIdentifierColumns(entity.getClass());

		return this.databaseClient.sql(operation) //
				.filter(statement -> {

					statement = statementFilterFunction.apply(statement);

					if (identifierColumns.isEmpty()) {
						return statement.returnGeneratedValues();
					}

					return statement.returnGeneratedValues(dataAccessStrategy.renderForGeneratedValues(identifierColumns.get(0)));
				}).map(this.dataAccessStrategy.getConverter().populateIdIfNecessary(entity)) //
				.all() //
				.last(entity).flatMap(saved -> maybeCallAfterSave(saved, outboundRow, tableName));
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

	@Override
	public <T> Mono<T> update(T entity) throws DataAccessException {

		Assert.notNull(entity, "Entity must not be null");

		return doUpdate(entity, getRequiredEntity(entity).getQualifiedTableName());
	}

	private <T> Mono<T> doUpdate(T entity, SqlIdentifier tableName) {

		RelationalPersistentEntity<T> persistentEntity = getRequiredEntity(entity);

		return maybeCallBeforeConvert(entity, tableName).flatMap(onBeforeConvert -> {

			T entityToUse;
			Criteria matchingVersionCriteria;

			if (persistentEntity.hasVersionProperty()) {

				matchingVersionCriteria = createMatchingVersionCriteria(onBeforeConvert, persistentEntity);
				entityToUse = incrementVersion(persistentEntity, onBeforeConvert);
			} else {

				entityToUse = onBeforeConvert;
				matchingVersionCriteria = null;
			}

			OutboundRow outboundRow = dataAccessStrategy.getOutboundRow(entityToUse);

			return maybeCallBeforeSave(entityToUse, outboundRow, tableName) //
					.flatMap(onBeforeSave -> {

						SqlIdentifier idColumn = persistentEntity.getRequiredIdProperty().getColumnName();
						Parameter id = outboundRow.remove(idColumn);

						persistentEntity.forEach(p -> {
							if (p.isInsertOnly()) {
								outboundRow.remove(p.getColumnName());
							}
						});

						Criteria criteria = Criteria.where(dataAccessStrategy.toSql(idColumn)).is(id);

						if (matchingVersionCriteria != null) {
							criteria = criteria.and(matchingVersionCriteria);
						}

						return doUpdate(onBeforeSave, tableName, persistentEntity, criteria, outboundRow);
					});
		});
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <T> Mono<T> doUpdate(T entity, SqlIdentifier tableName, RelationalPersistentEntity<T> persistentEntity,
			Criteria criteria, OutboundRow outboundRow) {

		Update update = Update.from((Map) outboundRow);

		StatementMapper mapper = dataAccessStrategy.getStatementMapper();
		StatementMapper.UpdateSpec updateSpec = mapper.createUpdate(tableName, update).withCriteria(criteria);

		PreparedOperation<?> operation = mapper.getMappedObject(updateSpec);

		return this.databaseClient.sql(operation) //
				.filter(statementFilterFunction) //
				.fetch() //
				.rowsUpdated() //
				.handle((rowsUpdated, sink) -> {

					if (rowsUpdated != 0) {
						return;
					}

					if (persistentEntity.hasVersionProperty()) {
						sink.error(new OptimisticLockingFailureException(
								formatOptimisticLockingExceptionMessage(entity, persistentEntity)));
					} else {
						sink.error(new TransientDataAccessResourceException(
								formatTransientEntityExceptionMessage(entity, persistentEntity)));
					}
				}).then(maybeCallAfterSave(entity, outboundRow, tableName));
	}

	private <T> String formatOptimisticLockingExceptionMessage(T entity, RelationalPersistentEntity<T> persistentEntity) {

		return String.format("Failed to update table [%s]; Version does not match for row with Id [%s]",
				persistentEntity.getQualifiedTableName(), persistentEntity.getIdentifierAccessor(entity).getIdentifier());
	}

	private <T> String formatTransientEntityExceptionMessage(T entity, RelationalPersistentEntity<T> persistentEntity) {

		return String.format("Failed to update table [%s]; Row with Id [%s] does not exist",
				persistentEntity.getQualifiedTableName(), persistentEntity.getIdentifierAccessor(entity).getIdentifier());
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

	@Override
	public <T> Mono<T> delete(T entity) throws DataAccessException {

		Assert.notNull(entity, "Entity must not be null");

		RelationalPersistentEntity<?> persistentEntity = getRequiredEntity(entity);

		return delete(getByIdQuery(entity, persistentEntity), persistentEntity.getType()).thenReturn(entity);
	}

	protected <T> Mono<T> maybeCallBeforeConvert(T object, SqlIdentifier table) {

		if (entityCallbacks != null) {
			return entityCallbacks.callback(BeforeConvertCallback.class, object, table);
		}

		return Mono.just(object);
	}

	protected <T> Mono<T> maybeCallBeforeSave(T object, OutboundRow row, SqlIdentifier table) {

		if (entityCallbacks != null) {
			return entityCallbacks.callback(BeforeSaveCallback.class, object, row, table);
		}

		return Mono.just(object);
	}

	protected <T> Mono<T> maybeCallAfterSave(T object, OutboundRow row, SqlIdentifier table) {

		if (entityCallbacks != null) {
			return entityCallbacks.callback(AfterSaveCallback.class, object, row, table);
		}

		return Mono.just(object);
	}

	protected <T> Mono<T> maybeCallAfterConvert(T object, SqlIdentifier table) {

		if (entityCallbacks != null) {
			return entityCallbacks.callback(AfterConvertCallback.class, object, table);
		}

		return Mono.just(object);
	}

	private <T> Query getByIdQuery(T entity, RelationalPersistentEntity<?> persistentEntity) {

		if (!persistentEntity.hasIdProperty()) {
			throw new MappingException("No id property found for object of type " + persistentEntity.getType());
		}

		IdentifierAccessor identifierAccessor = persistentEntity.getIdentifierAccessor(entity);
		Object id = identifierAccessor.getRequiredIdentifier();

		return Query.query(Criteria.where(persistentEntity.getRequiredIdProperty().getName()).is(id));
	}

	SqlIdentifier getTableName(Class<?> entityClass) {
		return getRequiredEntity(entityClass).getQualifiedTableName();
	}

	SqlIdentifier getTableNameOrEmpty(Class<?> entityClass) {

		RelationalPersistentEntity<?> entity = this.mappingContext.getPersistentEntity(entityClass);

		return entity != null ? entity.getQualifiedTableName() : SqlIdentifier.EMPTY;
	}

	private RelationalPersistentEntity<?> getRequiredEntity(Class<?> entityClass) {
		return this.mappingContext.getRequiredPersistentEntity(entityClass);
	}

	private <T> RelationalPersistentEntity<T> getRequiredEntity(T entity) {
		Class<?> entityType = ProxyUtils.getUserClass(entity);
		return (RelationalPersistentEntity) getRequiredEntity(entityType);
	}

	private <T> List<Expression> getSelectProjection(Table table, Query query, Class<?> entityType, Class<T> returnType) {

		if (query.getColumns().isEmpty()) {

			EntityProjection<T, ?> projection = converter.introspectProjection(returnType, entityType);

			if (projection.isProjection() && projection.isClosedProjection()) {

				return computeProjectedFields(table, returnType, projection);

			}

			return Collections.singletonList(table.asterisk());
		}

		return query.getColumns().stream().map(table::column).collect(Collectors.toList());
	}

	@SuppressWarnings("unchecked")
	private <T> List<Expression> computeProjectedFields(Table table, Class<T> returnType,
			EntityProjection<T, ?> projection) {

		if (returnType.isInterface()) {

			Set<String> properties = new LinkedHashSet<>();
			projection.forEach(it -> {
				properties.add(it.getPropertyPath().getSegment());
			});

			return properties.stream().map(table::column).collect(Collectors.toList());
		}

		Set<SqlIdentifier> properties = new LinkedHashSet<>();
		// DTO projections use merged metadata between domain type and result type
		PersistentPropertyTranslator translator = PersistentPropertyTranslator.create(
				mappingContext.getRequiredPersistentEntity(projection.getDomainType()),
				Predicates.negate(RelationalPersistentProperty::hasExplicitColumnName));

		RelationalPersistentEntity<?> persistentEntity = mappingContext
				.getRequiredPersistentEntity(projection.getMappedType());
		for (RelationalPersistentProperty property : persistentEntity) {
			properties.add(translator.translate(property).getColumnName());
		}

		return properties.stream().map(table::column).collect(Collectors.toList());
	}

	@SuppressWarnings("unchecked")
	public <T> RowsFetchSpec<T> getRowsFetchSpec(DatabaseClient.GenericExecuteSpec executeSpec, Class<?> entityType,
			Class<T> resultType) {

		boolean simpleType = getConverter().isSimpleType(resultType);

		BiFunction<Row, RowMetadata, T> rowMapper;

		// Bridge-code: Consider Converter<Row, T> until we have fully migrated to RowDocument
		if (converter instanceof AbstractRelationalConverter relationalConverter
				&& relationalConverter.getConversions().hasCustomReadTarget(Row.class, resultType)) {

			ConversionService conversionService = relationalConverter.getConversionService();
			rowMapper = (row, rowMetadata) -> (T) conversionService.convert(row, resultType);
		} else if (simpleType) {
			rowMapper = dataAccessStrategy.getRowMapper(resultType);
		} else {

			EntityProjection<T, ?> projection = converter.introspectProjection(resultType, entityType);
			Class<T> typeToRead = projection.isProjection() ? resultType
					: resultType.isInterface() ? (Class<T>) entityType : resultType;

			rowMapper = (row, rowMetadata) -> {

				RowDocument document = dataAccessStrategy.toRowDocument(typeToRead, row, rowMetadata.getColumnMetadatas());
				return converter.project(projection, document);
			};
		}

		// avoid top-level null values if the read type is a simple one (e.g. SELECT MAX(age) via Integer.class)
		if (simpleType) {
			return new UnwrapOptionalFetchSpecAdapter<>(
					executeSpec.map((row, metadata) -> Optional.ofNullable(rowMapper.apply(row, metadata))));
		}

		return executeSpec.map(rowMapper);
	}

	/**
	 * {@link RowsFetchSpec} adapter emitting values from {@link Optional} if they exist.
	 *
	 * @param <T>
	 */
	private static class UnwrapOptionalFetchSpecAdapter<T> implements RowsFetchSpec<T> {

		private final RowsFetchSpec<Optional<T>> delegate;

		private UnwrapOptionalFetchSpecAdapter(RowsFetchSpec<Optional<T>> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Mono<T> one() {
			return delegate.one().handle((optional, sink) -> optional.ifPresent(sink::next));
		}

		@Override
		public Mono<T> first() {
			return delegate.first().handle((optional, sink) -> optional.ifPresent(sink::next));
		}

		@Override
		public Flux<T> all() {
			return delegate.all().handle((optional, sink) -> optional.ifPresent(sink::next));
		}
	}

	/**
	 * {@link RowsFetchSpec} adapter applying {@link #maybeCallAfterConvert(Object, SqlIdentifier)} to each emitted
	 * object.
	 *
	 * @param <T>
	 */
	private class EntityCallbackAdapter<T> implements RowsFetchSpec<T> {

		private final RowsFetchSpec<T> delegate;
		private final SqlIdentifier tableName;

		private EntityCallbackAdapter(RowsFetchSpec<T> delegate, SqlIdentifier tableName) {
			this.delegate = delegate;
			this.tableName = tableName;
		}

		@Override
		public Mono<T> one() {
			return delegate.one().flatMap(it -> maybeCallAfterConvert(it, tableName));
		}

		@Override
		public Mono<T> first() {
			return delegate.first().flatMap(it -> maybeCallAfterConvert(it, tableName));
		}

		@Override
		public Flux<T> all() {
			return delegate.all().concatMap(it -> maybeCallAfterConvert(it, tableName));
		}
	}

}
