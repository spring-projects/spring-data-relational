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

import java.util.Optional;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.relational.core.conversion.AggregateChange;
import org.springframework.data.relational.core.conversion.AggregateChange.Kind;
import org.springframework.data.relational.core.conversion.Interpreter;
import org.springframework.data.relational.core.conversion.RelationalConverter;
import org.springframework.data.relational.core.conversion.RelationalEntityDeleteWriter;
import org.springframework.data.relational.core.conversion.RelationalEntityWriter;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.event.AfterDeleteEvent;
import org.springframework.data.relational.core.mapping.event.AfterLoadEvent;
import org.springframework.data.relational.core.mapping.event.AfterSaveEvent;
import org.springframework.data.relational.core.mapping.event.BeforeDeleteEvent;
import org.springframework.data.relational.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.relational.core.mapping.event.Identifier;
import org.springframework.data.relational.core.mapping.event.Identifier.Specified;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link JdbcAggregateOperations} implementation, storing aggregates in and obtaining them from a JDBC data store.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class JdbcAggregateTemplate implements JdbcAggregateOperations {

	private final ApplicationEventPublisher publisher;
	private final RelationalMappingContext context;
	private final RelationalConverter converter;
	private final Interpreter interpreter;

	private final RelationalEntityWriter jdbcEntityWriter;
	private final RelationalEntityDeleteWriter jdbcEntityDeleteWriter;

	private final DataAccessStrategy accessStrategy;

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
		this.jdbcEntityDeleteWriter = new RelationalEntityDeleteWriter(context);
		this.interpreter = new DefaultJdbcInterpreter(context, accessStrategy);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#save(java.lang.Object)
	 */
	@Override
	public <T> T save(T instance) {

		Assert.notNull(instance, "Aggregate instance must not be null!");

		RelationalPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(instance.getClass());
		IdentifierAccessor identifierAccessor = persistentEntity.getIdentifierAccessor(instance);

		AggregateChange<T> change = createChange(instance);

		publisher.publishEvent(new BeforeSaveEvent( //
				Identifier.ofNullable(identifierAccessor.getIdentifier()), //
				instance, //
				change //
		));

		change.executeWith(interpreter, context, converter);

		Object identifier = persistentEntity.getIdentifierAccessor(change.getEntity()).getIdentifier();

		Assert.notNull(identifier, "After saving the identifier must not be null");

		publisher.publishEvent(new AfterSaveEvent( //
				Identifier.of(identifier), //
				change.getEntity(), //
				change //
		));

		return (T) change.getEntity();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#count(java.lang.Class)
	 */
	@Override
	public long count(Class<?> domainType) {
		return accessStrategy.count(domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> T findById(Object id, Class<T> domainType) {

		T entity = accessStrategy.findById(id, domainType);
		if (entity != null) {
			publishAfterLoad(id, entity);
		}
		return entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#existsById(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return accessStrategy.existsById(id, domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findAll(java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {

		Iterable<T> all = accessStrategy.findAll(domainType);
		publishAfterLoad(all);
		return all;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#findAllById(java.lang.Iterable, java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {

		Iterable<T> allById = accessStrategy.findAllById(ids, domainType);
		publishAfterLoad(allById);
		return allById;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#delete(java.lang.Object, java.lang.Class)
	 */
	@Override
	public <S> void delete(S aggregateRoot, Class<S> domainType) {

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
		deleteTree(id, null, domainType);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.jdbc.core.JdbcAggregateOperations#deleteAll(java.lang.Class)
	 */
	@Override
	public void deleteAll(Class<?> domainType) {

		AggregateChange<?> change = createDeletingChange(domainType);
		change.executeWith(interpreter, context, converter);
	}

	private void deleteTree(Object id, @Nullable Object entity, Class<?> domainType) {

		AggregateChange<?> change = createDeletingChange(id, entity, domainType);

		Specified specifiedId = Identifier.of(id);
		Optional<Object> optionalEntity = Optional.ofNullable(entity);
		publisher.publishEvent(new BeforeDeleteEvent(specifiedId, optionalEntity, change));

		change.executeWith(interpreter, context, converter);

		publisher.publishEvent(new AfterDeleteEvent(specifiedId, optionalEntity, change));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <T> AggregateChange<T> createChange(T instance) {

		AggregateChange<T> aggregateChange = new AggregateChange(Kind.SAVE, instance.getClass(), instance);
		jdbcEntityWriter.write(instance, aggregateChange);
		return aggregateChange;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private AggregateChange<?> createDeletingChange(Object id, @Nullable Object entity, Class<?> domainType) {

		AggregateChange<?> aggregateChange = new AggregateChange(Kind.DELETE, domainType, entity);
		jdbcEntityDeleteWriter.write(id, aggregateChange);
		return aggregateChange;
	}

	private AggregateChange<?> createDeletingChange(Class<?> domainType) {

		AggregateChange<?> aggregateChange = new AggregateChange<>(Kind.DELETE, domainType, null);
		jdbcEntityDeleteWriter.write(null, aggregateChange);
		return aggregateChange;
	}

	private <T> void publishAfterLoad(Iterable<T> all) {

		for (T e : all) {

			RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(e.getClass());
			IdentifierAccessor identifierAccessor = entity.getIdentifierAccessor(e);

			publishAfterLoad(identifierAccessor.getRequiredIdentifier(), e);
		}
	}

	private <T> void publishAfterLoad(Object id, T entity) {
		publisher.publishEvent(new AfterLoadEvent(Identifier.of(id), entity));
	}
}
