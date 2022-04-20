/*
 * Copyright 2017-2022 the original author or authors.
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
package org.springframework.data.jdbc.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.relational.core.conversion.AggregateChange;
import org.springframework.data.relational.core.conversion.BatchingAggregateChange;
import org.springframework.data.relational.core.conversion.DeleteAggregateChange;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.conversion.RelationalEntityDeleteWriter;
import org.springframework.data.relational.core.conversion.RelationalEntityInsertWriter;
import org.springframework.data.relational.core.conversion.RelationalEntityUpdateWriter;
import org.springframework.data.relational.core.conversion.RelationalEntityVersionUtils;
import org.springframework.data.relational.core.conversion.RootAggregateChange;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.mapping.event.*;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * {@link JdbcAggregateOperations} implementation, storing aggregates in and obtaining them from a JDBC data store.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Thomas Lang
 * @author Christoph Strobl
 * @author Milan Milanov
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 */
public class JdbcAggregateTemplate implements JdbcAggregateOperations {

	private final ApplicationEventPublisher publisher;
	private final RelationalMappingContext context;

	private final RelationalEntityDeleteWriter jdbcEntityDeleteWriter;

	private final DataAccessStrategy accessStrategy;
	private final AggregateChangeExecutor executor;
	private final JdbcConverter converter;

	private EntityCallbacks entityCallbacks = EntityCallbacks.create();

	/**
	 * Creates a new {@link JdbcAggregateTemplate} given {@link ApplicationContext}, {@link RelationalMappingContext} and
	 * {@link DataAccessStrategy}.
	 *
	 * @param publisher must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 * @since 1.1
	 */
	public JdbcAggregateTemplate(ApplicationContext publisher, RelationalMappingContext context, JdbcConverter converter,
			DataAccessStrategy dataAccessStrategy) {

		Assert.notNull(publisher, "ApplicationContext must not be null!");
		Assert.notNull(context, "RelationalMappingContext must not be null!");
		Assert.notNull(converter, "RelationalConverter must not be null!");
		Assert.notNull(dataAccessStrategy, "DataAccessStrategy must not be null!");

		this.publisher = publisher;
		this.context = context;
		this.accessStrategy = dataAccessStrategy;
		this.converter = converter;

		this.jdbcEntityDeleteWriter = new RelationalEntityDeleteWriter(context);

		this.executor = new AggregateChangeExecutor(converter, accessStrategy);

		setEntityCallbacks(EntityCallbacks.create(publisher));
	}

	/**
	 * Creates a new {@link JdbcAggregateTemplate} given {@link ApplicationEventPublisher},
	 * {@link RelationalMappingContext} and {@link DataAccessStrategy}.
	 *
	 * @param publisher must not be {@literal null}.
	 * @param context must not be {@literal null}.
	 * @param dataAccessStrategy must not be {@literal null}.
	 */
	public JdbcAggregateTemplate(ApplicationEventPublisher publisher, RelationalMappingContext context,
			JdbcConverter converter, DataAccessStrategy dataAccessStrategy) {

		Assert.notNull(publisher, "ApplicationEventPublisher must not be null!");
		Assert.notNull(context, "RelationalMappingContext must not be null!");
		Assert.notNull(converter, "RelationalConverter must not be null!");
		Assert.notNull(dataAccessStrategy, "DataAccessStrategy must not be null!");

		this.publisher = publisher;
		this.context = context;
		this.accessStrategy = dataAccessStrategy;
		this.converter = converter;

		this.jdbcEntityDeleteWriter = new RelationalEntityDeleteWriter(context);
		this.executor = new AggregateChangeExecutor(converter, accessStrategy);
	}

	/**
	 * Sets the callbacks to be invoked on life cycle events.
	 *
	 * @param entityCallbacks must not be {@literal null}.
	 * @since 1.1
	 */
	public void setEntityCallbacks(EntityCallbacks entityCallbacks) {

		Assert.notNull(entityCallbacks, "Callbacks must not be null.");

		this.entityCallbacks = entityCallbacks;
	}

	@Override
	public <T> T save(T instance) {

		Assert.notNull(instance, "Aggregate instance must not be null!");

		return performSave(instance, changeCreatorSelectorForSave(instance));
	}

	@Override
	public <T> Iterable<T> saveAll(Iterable<T> instances) {

		Assert.isTrue(instances.iterator().hasNext(), "Aggregate instances must not be empty!");

		return performSaveAll(instances);
	}

	/**
	 * Dedicated insert function to do just the insert of an instance of an aggregate, including all the members of the
	 * aggregate.
	 *
	 * @param instance the aggregate root of the aggregate to be inserted. Must not be {@code null}.
	 * @return the saved instance.
	 */
	@Override
	public <T> T insert(T instance) {

		Assert.notNull(instance, "Aggregate instance must not be null!");

		return performSave(instance, entity -> createInsertChange(prepareVersionForInsert(entity)));
	}

	/**
	 * Dedicated update function to do just an update of an instance of an aggregate, including all the members of the
	 * aggregate.
	 *
	 * @param instance the aggregate root of the aggregate to be inserted. Must not be {@code null}.
	 * @return the saved instance.
	 */
	@Override
	public <T> T update(T instance) {

		Assert.notNull(instance, "Aggregate instance must not be null!");

		return performSave(instance, entity -> createUpdateChange(prepareVersionForUpdate(entity)));
	}

	@Override
	public long count(Class<?> domainType) {

		Assert.notNull(domainType, "Domain type must not be null");

		return accessStrategy.count(domainType);
	}

	@Override
	public <T> T findById(Object id, Class<T> domainType) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		T entity = accessStrategy.findById(id, domainType);
		if (entity == null) {
			return null;
		}
		return triggerAfterConvert(entity);
	}

	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		return accessStrategy.existsById(id, domainType);
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType, Sort sort) {

		Assert.notNull(domainType, "Domain type must not be null!");

		Iterable<T> all = accessStrategy.findAll(domainType, sort);
		return triggerAfterConvert(all);
	}

	@Override
	public <T> Page<T> findAll(Class<T> domainType, Pageable pageable) {

		Assert.notNull(domainType, "Domain type must not be null!");

		Iterable<T> items = triggerAfterConvert(accessStrategy.findAll(domainType, pageable));
		List<T> content = StreamSupport.stream(items.spliterator(), false).collect(Collectors.toList());

		return PageableExecutionUtils.getPage(content, pageable, () -> accessStrategy.count(domainType));
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {

		Assert.notNull(domainType, "Domain type must not be null!");

		Iterable<T> all = accessStrategy.findAll(domainType);
		return triggerAfterConvert(all);
	}

	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {

		Assert.notNull(ids, "Ids must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		Iterable<T> allById = accessStrategy.findAllById(ids, domainType);
		return triggerAfterConvert(allById);
	}

	@Override
	public <S> void delete(S aggregateRoot, Class<S> domainType) {

		Assert.notNull(aggregateRoot, "Aggregate root must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		IdentifierAccessor identifierAccessor = context.getRequiredPersistentEntity(domainType)
				.getIdentifierAccessor(aggregateRoot);

		deleteTree(identifierAccessor.getRequiredIdentifier(), aggregateRoot, domainType);
	}

	@Override
	public <S> void deleteById(Object id, Class<S> domainType) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		deleteTree(id, null, domainType);
	}

	@Override
	public <T> void deleteAllById(Iterable<?> ids, Class<T> domainType) {

		Assert.isTrue(ids.iterator().hasNext(), "Ids must not be empty!");

		BatchingAggregateChange<T, DeleteAggregateChange<T>> batchingAggregateChange = BatchingAggregateChange
				.forDelete(domainType);

		ids.forEach(id -> {
			DeleteAggregateChange<T> change = createDeletingChange(id, null, domainType);
			triggerBeforeDelete(null, id, change);
			batchingAggregateChange.add(change);
		});

		executor.executeDelete(batchingAggregateChange);

		ids.forEach(id -> triggerAfterDelete(null, id, batchingAggregateChange));
	}

	@Override
	public void deleteAll(Class<?> domainType) {

		Assert.notNull(domainType, "Domain type must not be null!");

		MutableAggregateChange<?> change = createDeletingChange(domainType);
		executor.executeDelete(change);
	}

	@Override
	public <T> void deleteAll(Iterable<? extends T> instances, Class<T> domainType) {

		Assert.isTrue(instances.iterator().hasNext(), "Aggregate instances must not be empty!");

		BatchingAggregateChange<T, DeleteAggregateChange<T>> batchingAggregateChange = BatchingAggregateChange
				.forDelete(domainType);
		Map<Object, T> instancesBeforeExecute = new LinkedHashMap<>();

		instances.forEach(instance -> {
			Object id = context.getRequiredPersistentEntity(domainType).getIdentifierAccessor(instance)
					.getRequiredIdentifier();
			DeleteAggregateChange<T> change = createDeletingChange(id, instance, domainType);
			instancesBeforeExecute.put(id, triggerBeforeDelete(instance, id, change));
			batchingAggregateChange.add(change);
		});

		executor.executeDelete(batchingAggregateChange);

		instancesBeforeExecute.forEach((id, instance) -> triggerAfterDelete(instance, id, batchingAggregateChange));
	}

	private <T> T afterExecute(AggregateChange<T> change, T entityAfterExecution) {

		Object identifier = context.getRequiredPersistentEntity(change.getEntityType())
				.getIdentifierAccessor(entityAfterExecution).getIdentifier();

		Assert.notNull(identifier, "After saving the identifier must not be null!");

		return triggerAfterSave(entityAfterExecution, change);
	}

	private <T> RootAggregateChange<T> beforeExecute(T aggregateRoot, Function<T, RootAggregateChange<T>> changeCreator) {

		Assert.notNull(aggregateRoot, "Aggregate instance must not be null!");

		aggregateRoot = triggerBeforeConvert(aggregateRoot);

		RootAggregateChange<T> change = changeCreator.apply(aggregateRoot);

		aggregateRoot = triggerBeforeSave(change.getRoot(), change);

		change.setRoot(aggregateRoot);

		return change;
	}

	private <T> void deleteTree(Object id, @Nullable T entity, Class<T> domainType) {

		MutableAggregateChange<T> change = createDeletingChange(id, entity, domainType);

		entity = triggerBeforeDelete(entity, id, change);

		executor.executeDelete(change);

		triggerAfterDelete(entity, id, change);
	}

	private <T> T performSave(T instance, Function<T, RootAggregateChange<T>> changeCreator) {

		// noinspection unchecked
		BatchingAggregateChange<T, RootAggregateChange<T>> batchingAggregateChange = //
				BatchingAggregateChange.forSave((Class<T>) ClassUtils.getUserClass(instance));
		batchingAggregateChange.add(beforeExecute(instance, changeCreator));

		Iterator<T> afterExecutionIterator = executor.executeSave(batchingAggregateChange).iterator();

		Assert.isTrue(afterExecutionIterator.hasNext(), "Instances after execution must not be empty!");

		return afterExecute(batchingAggregateChange, afterExecutionIterator.next());
	}

	private <T> List<T> performSaveAll(Iterable<T> instances) {

		BatchingAggregateChange<T, RootAggregateChange<T>> batchingAggregateChange = null;

		for (T instance : instances) {
			if (batchingAggregateChange == null) {
				batchingAggregateChange = BatchingAggregateChange.forSave((Class<T>) ClassUtils.getUserClass(instance));
			}
			batchingAggregateChange.add(beforeExecute(instance, changeCreatorSelectorForSave(instance)));
		}

		Assert.notNull(batchingAggregateChange, "Iterable in saveAll must not be empty");

		List<T> instancesAfterExecution = executor.executeSave(batchingAggregateChange);

		ArrayList<T> results = new ArrayList<>(instancesAfterExecution.size());
		for (T instance : instancesAfterExecution) {
			results.add(afterExecute(batchingAggregateChange, instance));
		}

		return results;
	}

	private <T> Function<T, RootAggregateChange<T>> changeCreatorSelectorForSave(T instance) {

		return context.getRequiredPersistentEntity(instance.getClass()).isNew(instance)
				? entity -> createInsertChange(prepareVersionForInsert(entity))
				: entity -> createUpdateChange(prepareVersionForUpdate(entity));
	}

	private <T> RootAggregateChange<T> createInsertChange(T instance) {

		RootAggregateChange<T> aggregateChange = MutableAggregateChange.forSave(instance);
		new RelationalEntityInsertWriter<T>(context).write(instance, aggregateChange);
		return aggregateChange;
	}

	private <T> RootAggregateChange<T> createUpdateChange(EntityAndPreviousVersion<T> entityAndVersion) {

		RootAggregateChange<T> aggregateChange = MutableAggregateChange.forSave(entityAndVersion.entity,
				entityAndVersion.version);
		new RelationalEntityUpdateWriter<T>(context).write(entityAndVersion.entity, aggregateChange);
		return aggregateChange;
	}

	private <T> T prepareVersionForInsert(T instance) {

		RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(instance);
		T preparedInstance = instance;
		if (persistentEntity.hasVersionProperty()) {
			RelationalPersistentProperty versionProperty = persistentEntity.getRequiredVersionProperty();

			long initialVersion = versionProperty.getActualType().isPrimitive() ? 1L : 0;

			preparedInstance = RelationalEntityVersionUtils.setVersionNumberOnEntity( //
					instance, initialVersion, persistentEntity, converter);
		}
		return preparedInstance;
	}

	private <T> EntityAndPreviousVersion<T> prepareVersionForUpdate(T instance) {

		RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(instance);
		T preparedInstance = instance;
		Number previousVersion = null;
		if (persistentEntity.hasVersionProperty()) {
			// If the root aggregate has a version property, increment it.
			previousVersion = RelationalEntityVersionUtils.getVersionNumberFromEntity(instance, persistentEntity, converter);

			long newVersion = (previousVersion == null ? 0 : previousVersion.longValue()) + 1;

			preparedInstance = RelationalEntityVersionUtils.setVersionNumberOnEntity(instance, newVersion, persistentEntity,
					converter);
		}
		return new EntityAndPreviousVersion<>(preparedInstance, previousVersion);
	}

	@SuppressWarnings("unchecked")
	private <T> RelationalPersistentEntity<T> getRequiredPersistentEntity(T instance) {
		return (RelationalPersistentEntity<T>) context.getRequiredPersistentEntity(instance.getClass());
	}

	private <T> DeleteAggregateChange<T> createDeletingChange(Object id, @Nullable T entity, Class<T> domainType) {

		Number previousVersion = null;
		if (entity != null) {
			RelationalPersistentEntity<T> persistentEntity = getRequiredPersistentEntity(entity);
			if (persistentEntity.hasVersionProperty()) {
				previousVersion = RelationalEntityVersionUtils.getVersionNumberFromEntity(entity, persistentEntity, converter);
			}
		}
		DeleteAggregateChange<T> aggregateChange = MutableAggregateChange.forDelete(domainType, previousVersion);
		jdbcEntityDeleteWriter.write(id, aggregateChange);
		return aggregateChange;
	}

	private MutableAggregateChange<?> createDeletingChange(Class<?> domainType) {

		MutableAggregateChange<?> aggregateChange = MutableAggregateChange.forDelete(domainType);
		jdbcEntityDeleteWriter.write(null, aggregateChange);
		return aggregateChange;
	}

	private <T> Iterable<T> triggerAfterConvert(Iterable<T> all) {

		List<T> result = new ArrayList<>();

		for (T e : all) {
			result.add(triggerAfterConvert(e));
		}

		return result;
	}

	private <T> T triggerAfterConvert(T entity) {

		publisher.publishEvent(new AfterConvertEvent<>(entity));
		return entityCallbacks.callback(AfterConvertCallback.class, entity);
	}

	private <T> T triggerBeforeConvert(T aggregateRoot) {

		publisher.publishEvent(new BeforeConvertEvent<>(aggregateRoot));

		return entityCallbacks.callback(BeforeConvertCallback.class, aggregateRoot);
	}

	private <T> T triggerBeforeSave(T aggregateRoot, AggregateChange<T> change) {

		publisher.publishEvent(new BeforeSaveEvent<>(aggregateRoot, change));

		return entityCallbacks.callback(BeforeSaveCallback.class, aggregateRoot, change);
	}

	private <T> T triggerAfterSave(T aggregateRoot, AggregateChange<T> change) {

		publisher.publishEvent(new AfterSaveEvent<>(aggregateRoot, change));

		return entityCallbacks.callback(AfterSaveCallback.class, aggregateRoot);
	}

	private <T> void triggerAfterDelete(@Nullable T aggregateRoot, Object id, AggregateChange<T> change) {

		publisher.publishEvent(new AfterDeleteEvent<>(Identifier.of(id), aggregateRoot, change));

		if (aggregateRoot != null) {
			entityCallbacks.callback(AfterDeleteCallback.class, aggregateRoot);
		}
	}

	@Nullable
	private <T> T triggerBeforeDelete(@Nullable T aggregateRoot, Object id, MutableAggregateChange<T> change) {

		publisher.publishEvent(new BeforeDeleteEvent<>(Identifier.of(id), aggregateRoot, change));

		if (aggregateRoot != null) {
			return entityCallbacks.callback(BeforeDeleteCallback.class, aggregateRoot, change);
		}

		return null;
	}

	private static class EntityAndPreviousVersion<T> {

		private final T entity;
		private final Number version;

		EntityAndPreviousVersion(T entity, @Nullable Number version) {

			this.entity = entity;
			this.version = version;
		}
	}
}
