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
import org.springframework.data.relational.core.conversion.Interpreter;
import org.springframework.data.relational.core.conversion.RelationalEntityDeleteWriter;
import org.springframework.data.relational.core.conversion.RelationalEntityWriter;
import org.springframework.data.relational.core.conversion.AggregateChange.Kind;
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
	private final Interpreter interpreter;

	private final RelationalEntityWriter jdbcEntityWriter;
	private final RelationalEntityDeleteWriter jdbcEntityDeleteWriter;

	private final DataAccessStrategy accessStrategy;

	public JdbcAggregateTemplate(ApplicationEventPublisher publisher, RelationalMappingContext context,
			DataAccessStrategy dataAccessStrategy) {

		this.publisher = publisher;
		this.context = context;

		this.jdbcEntityWriter = new RelationalEntityWriter(context);
		this.jdbcEntityDeleteWriter = new RelationalEntityDeleteWriter(context);
		this.accessStrategy = dataAccessStrategy;
		this.interpreter = new DefaultJdbcInterpreter(context, accessStrategy);
	}

	@Override
	public <T> void save(T instance) {

		Assert.notNull(instance, "Aggregate instance must not be null!");

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(instance.getClass());
		IdentifierAccessor identifierAccessor = entity.getIdentifierAccessor(instance);

		AggregateChange change = createChange(instance);

		publisher.publishEvent(new BeforeSaveEvent( //
				Identifier.ofNullable(identifierAccessor.getIdentifier()), //
				instance, //
				change //
		));

		change.executeWith(interpreter);

		Object identifier = identifierAccessor.getIdentifier();

		Assert.notNull(identifier, "After saving the identifier must not be null");

		publisher.publishEvent(new AfterSaveEvent( //
				Identifier.of(identifier), //
				instance, //
				change //
		));
	}

	@Override
	public long count(Class<?> domainType) {
		return accessStrategy.count(domainType);
	}

	@Override
	public <T> T findById(Object id, Class<T> domainType) {

		T entity = accessStrategy.findById(id, domainType);
		if (entity != null) {
			publishAfterLoad(id, entity);
		}
		return entity;
	}

	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return accessStrategy.existsById(id, domainType);
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {

		Iterable<T> all = accessStrategy.findAll(domainType);
		publishAfterLoad(all);
		return all;
	}

	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {

		Iterable<T> allById = accessStrategy.findAllById(ids, domainType);
		publishAfterLoad(allById);
		return allById;
	}

	@Override
	public <S> void delete(S aggregateRoot, Class<S> domainType) {

		IdentifierAccessor identifierAccessor = context.getRequiredPersistentEntity(domainType)
				.getIdentifierAccessor(aggregateRoot);

		deleteTree(identifierAccessor.getRequiredIdentifier(), aggregateRoot, domainType);
	}

	@Override
	public <S> void deleteById(Object id, Class<S> domainType) {
		deleteTree(id, null, domainType);
	}

	@Override
	public void deleteAll(Class<?> domainType) {

		AggregateChange change = createDeletingChange(domainType);
		change.executeWith(interpreter);
	}

	private void deleteTree(Object id, @Nullable Object entity, Class<?> domainType) {

		AggregateChange change = createDeletingChange(id, entity, domainType);

		Specified specifiedId = Identifier.of(id);
		Optional<Object> optionalEntity = Optional.ofNullable(entity);
		publisher.publishEvent(new BeforeDeleteEvent(specifiedId, optionalEntity, change));

		change.executeWith(interpreter);

		publisher.publishEvent(new AfterDeleteEvent(specifiedId, optionalEntity, change));
	}

	@SuppressWarnings("unchecked")
	private <T> AggregateChange createChange(T instance) {

		AggregateChange<?> aggregateChange = new AggregateChange(Kind.SAVE, instance.getClass(), instance);
		jdbcEntityWriter.write(instance, aggregateChange);
		return aggregateChange;
	}

	@SuppressWarnings("unchecked")
	private AggregateChange createDeletingChange(Object id, @Nullable Object entity, Class<?> domainType) {

		AggregateChange<?> aggregateChange = new AggregateChange(Kind.DELETE, domainType, entity);
		jdbcEntityDeleteWriter.write(id, aggregateChange);
		return aggregateChange;
	}

	private AggregateChange createDeletingChange(Class<?> domainType) {

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
