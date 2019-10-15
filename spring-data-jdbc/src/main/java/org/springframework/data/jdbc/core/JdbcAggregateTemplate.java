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
package org.springframework.data.jdbc.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.relational.core.conversion.AggregateChange;
import org.springframework.data.relational.core.conversion.Interpreter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalEntityDeleteWriter;
import org.springframework.data.relational.core.conversion.RelationalEntityInsertWriter;
import org.springframework.data.relational.core.conversion.RelationalEntityUpdateWriter;
import org.springframework.data.relational.core.conversion.RelationalEntityWriter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.event.*;
import org.springframework.data.relational.core.mapping.event.Identifier.Specified;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link JdbcAggregateOperations} implementation, storing aggregates in and obtaining them from a JDBC data store.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Thomas Lang
 * @author Christoph Strobl
 */
public class JdbcAggregateTemplate implements JdbcAggregateOperations {

	private final ApplicationEventPublisher publisher;
	private final RelationalMappingContext context;
	private final RelationalConverter converter;
	private final Interpreter interpreter;

	private final RelationalEntityWriter jdbcEntityWriter;
	private final RelationalEntityDeleteWriter jdbcEntityDeleteWriter;
	private final RelationalEntityInsertWriter jdbcEntityInsertWriter;
	private final RelationalEntityUpdateWriter jdbcEntityUpdateWriter;

	private final DataAccessStrategy accessStrategy;

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
	public JdbcAggregateTemplate(ApplicationContext publisher, RelationalMappingContext context,
			RelationalConverter converter, DataAccessStrategy dataAccessStrategy) {

		Assert.notNull(publisher, "ApplicationContext must not be null!");
		Assert.notNull(context, "RelationalMappingContext must not be null!");
		Assert.notNull(converter, "RelationalConverter must not be null!");
		Assert.notNull(dataAccessStrategy, "DataAccessStrategy must not be null!");

		this.publisher = publisher;
		this.context = context;
		this.converter = converter;
		this.accessStrategy = dataAccessStrategy;

		this.jdbcEntityWriter = new RelationalEntityWriter(context);
		this.jdbcEntityInsertWriter = new RelationalEntityInsertWriter(context);
		this.jdbcEntityUpdateWriter = new RelationalEntityUpdateWriter(context);
		this.jdbcEntityDeleteWriter = new RelationalEntityDeleteWriter(context);
		this.interpreter = new DefaultJdbcInterpreter(context, accessStrategy);

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
			RelationalConverter converter, DataAccessStrategy dataAccessStrategy) {

		Assert.notNull(publisher, "ApplicationEventPublisher must not be null!");
		Assert.notNull(context, "RelationalMappingContext must not be null!");
		Assert.notNull(converter, "RelationalConverter must not be null!");
		Assert.notNull(dataAccessStrategy, "DataAccessStrategy must not be null!");

		this.publisher = publisher;
		this.context = context;
		this.converter = converter;
		this.accessStrategy = dataAccessStrategy;

		this.jdbcEntityWriter = new RelationalEntityWriter(context);
		this.jdbcEntityInsertWriter = new RelationalEntityInsertWriter(context);
		this.jdbcEntityUpdateWriter = new RelationalEntityUpdateWriter(context);
		this.jdbcEntityDeleteWriter = new RelationalEntityDeleteWriter(context);
		this.interpreter = new DefaultJdbcInterpreter(context, accessStrategy);
	}

	/**
	 * @param entityCallbacks
	 * @since 1.1
	 */
	public void setEntityCallbacks(EntityCallbacks entityCallbacks) {

		Assert.notNull(entityCallbacks, "Callbacks must not be null.");

		this.entityCallbacks = entityCallbacks;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#save(java.lang.Object)
	 */
	@Override
	public <T> T save(T instance) {

		Assert.notNull(instance, "Aggregate instance must not be null!");

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(instance.getClass());

		Function<T, AggregateChange<T>> changeCreator = persistentEntity.isNew(instance) ? this::createInsertChange
				: this::createUpdateChange;

		return store(instance, changeCreator, persistentEntity);
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

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(instance.getClass());

		return store(instance, this::createInsertChange, persistentEntity);
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

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(instance.getClass());

		return store(instance, this::createUpdateChange, persistentEntity);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#count(java.lang.Class)
	 */
	@Override
	public long count(Class<?> domainType) {

		Assert.notNull(domainType, "Domain type must not be null");

		return accessStrategy.count(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> T findById(Object id, Class<T> domainType) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		T entity = accessStrategy.findById(id, domainType);
		if (entity != null) {
			return triggerAfterLoad(id, entity);
		}
		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#existsById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		return accessStrategy.existsById(id, domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {

		Assert.notNull(domainType, "Domain type must not be null!");

		Iterable<T> all = accessStrategy.findAll(domainType);
		return triggerAfterLoad(all);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findAllById(java.lang.Iterable, java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {

		Assert.notNull(ids, "Ids must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		Iterable<T> allById = accessStrategy.findAllById(ids, domainType);
		return triggerAfterLoad(allById);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#delete(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <S> void delete(S aggregateRoot, Class<S> domainType) {

		Assert.notNull(aggregateRoot, "Aggregate root must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		IdentifierAccessor identifierAccessor = context.getRequiredPersistentEntity(domainType)
				.getIdentifierAccessor(aggregateRoot);

		deleteTree(identifierAccessor.getRequiredIdentifier(), aggregateRoot, domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#deleteById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <S> void deleteById(Object id, Class<S> domainType) {

		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(domainType, "Domain type must not be null!");

		deleteTree(id, null, domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#deleteAll(java.lang.Class)
	 */
	@Override
	public void deleteAll(Class<?> domainType) {

		Assert.notNull(domainType, "Domain type must not be null!");

		AggregateChange<?> change = createDeletingChange(domainType);
		change.executeWith(interpreter, context, converter);
	}

	private <T> T store(T aggregateRoot, Function<T, AggregateChange<T>> changeCreator,
			RelationalPersistentEntity<?> persistentEntity) {

		Assert.notNull(aggregateRoot, "Aggregate instance must not be null!");

		aggregateRoot = triggerBeforeConvert(aggregateRoot);

		AggregateChange<T> change = changeCreator.apply(aggregateRoot);

		aggregateRoot = triggerBeforeSave(aggregateRoot,
				persistentEntity.getIdentifierAccessor(aggregateRoot).getIdentifier(), change);

		change.setEntity(aggregateRoot);

		change.executeWith(interpreter, context, converter);

		Object identifier = persistentEntity.getIdentifierAccessor(change.getEntity()).getIdentifier();

		Assert.notNull(identifier, "After saving the identifier must not be null!");

		return triggerAfterSave(change.getEntity(), identifier, change);
	}

	private <T> void deleteTree(Object id, @Nullable T entity, Class<T> domainType) {

		AggregateChange<T> change = createDeletingChange(id, entity, domainType);

		entity = triggerBeforeDelete(entity, id, change);
		change.setEntity(entity);

		change.executeWith(interpreter, context, converter);

		triggerAfterDelete(entity, id, change);
	}

	private <T> AggregateChange<T> createInsertChange(T instance) {

		AggregateChange<T> aggregateChange = AggregateChange.forSave(instance);
		jdbcEntityInsertWriter.write(instance, aggregateChange);
		return aggregateChange;
	}

	private <T> AggregateChange<T> createUpdateChange(T instance) {

		AggregateChange<T> aggregateChange = AggregateChange.forSave(instance);
		jdbcEntityUpdateWriter.write(instance, aggregateChange);
		return aggregateChange;
	}

	private <T> AggregateChange<T> createDeletingChange(Object id, @Nullable T entity, Class<T> domainType) {

		AggregateChange<T> aggregateChange = AggregateChange.forDelete(domainType, entity);
		jdbcEntityDeleteWriter.write(id, aggregateChange);
		return aggregateChange;
	}

	private AggregateChange<?> createDeletingChange(Class<?> domainType) {

		AggregateChange<?> aggregateChange = AggregateChange.forDelete(domainType, null);
		jdbcEntityDeleteWriter.write(null, aggregateChange);
		return aggregateChange;
	}

	private <T> Iterable<T> triggerAfterLoad(Iterable<T> all) {

		List<T> result = new ArrayList<>();

		for (T e : all) {

			RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(e.getClass());
			IdentifierAccessor identifierAccessor = entity.getIdentifierAccessor(e);

			result.add(triggerAfterLoad(identifierAccessor.getRequiredIdentifier(), e));
		}

		return result;
	}

	private <T> T triggerAfterLoad(Object id, T entity) {

		publisher.publishEvent(new AfterLoadEvent(Identifier.of(id), entity));

		return entityCallbacks.callback(AfterLoadCallback.class, entity);
	}

	private <T> T triggerBeforeConvert(T aggregateRoot) {
		return entityCallbacks.callback(BeforeConvertCallback.class, aggregateRoot);
	}

	private <T> T triggerBeforeSave(T aggregateRoot, @Nullable Object id, AggregateChange<T> change) {

		publisher.publishEvent(new BeforeSaveEvent( //
				Identifier.ofNullable(id), //
				aggregateRoot, //
				change //
		));

		return entityCallbacks.callback(BeforeSaveCallback.class, aggregateRoot, change);
	}

	private <T> T triggerAfterSave(T aggregateRoot, Object id, AggregateChange<T> change) {

		Specified identifier = Identifier.of(id);

		publisher.publishEvent(new AfterSaveEvent( //
				identifier, //
				aggregateRoot, //
				change //
		));

		return entityCallbacks.callback(AfterSaveCallback.class, aggregateRoot);
	}

	private <T> void triggerAfterDelete(@Nullable T aggregateRoot, Object id, AggregateChange<?> change) {

		publisher.publishEvent(new AfterDeleteEvent(Identifier.of(id), Optional.ofNullable(aggregateRoot), change));

		if (aggregateRoot != null) {
			entityCallbacks.callback(AfterDeleteCallback.class, aggregateRoot);
		}
	}

	@Nullable
	private <T> T triggerBeforeDelete(@Nullable T aggregateRoot, Object id, AggregateChange<?> change) {

		publisher.publishEvent(new BeforeDeleteEvent(Identifier.of(id), Optional.ofNullable(aggregateRoot), change));

		if (aggregateRoot != null) {
			return entityCallbacks.callback(BeforeDeleteCallback.class, aggregateRoot, change);
		}

		return null;
	}
}
