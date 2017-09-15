/*
 * Copyright 2017 the original author or authors.
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
import org.springframework.data.jdbc.core.conversion.AggregateChange;
import org.springframework.data.jdbc.core.conversion.AggregateChange.Kind;
import org.springframework.data.jdbc.core.conversion.Interpreter;
import org.springframework.data.jdbc.core.conversion.JdbcEntityDeleteWriter;
import org.springframework.data.jdbc.core.conversion.JdbcEntityWriter;
import org.springframework.data.jdbc.mapping.event.AfterDelete;
import org.springframework.data.jdbc.mapping.event.AfterSave;
import org.springframework.data.jdbc.mapping.event.BeforeDelete;
import org.springframework.data.jdbc.mapping.event.BeforeSave;
import org.springframework.data.jdbc.mapping.event.Identifier;
import org.springframework.data.jdbc.mapping.event.Identifier.Specified;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntityInformation;

/**
 * {@link JdbcEntityOperations} implementation, storing aggregates in and obtaining them from a JDBC data store.
 *
 * @author Jens Schauder
 */
public class JdbcEntityTemplate implements JdbcEntityOperations {

	private final ApplicationEventPublisher publisher;
	private final JdbcMappingContext context;
	private final Interpreter interpreter;

	private final JdbcEntityWriter jdbcEntityWriter;
	private final JdbcEntityDeleteWriter jdbcEntityDeleteWriter;

	private final DataAccessStrategy accessStrategy;

	public JdbcEntityTemplate(ApplicationEventPublisher publisher, JdbcMappingContext context,
			DataAccessStrategy dataAccessStrategy) {

		this.publisher = publisher;
		this.context = context;

		this.jdbcEntityWriter = new JdbcEntityWriter(context);
		this.jdbcEntityDeleteWriter = new JdbcEntityDeleteWriter(context);
		this.accessStrategy = dataAccessStrategy;
		this.interpreter = new DefaultJdbcInterpreter(context, accessStrategy);
	}

	@Override
	public <T> void save(T instance, Class<T> domainType) {

		JdbcPersistentEntityInformation<T, ?> entityInformation = context
				.getRequiredPersistentEntityInformation(domainType);

		AggregateChange change = createChange(instance);

		publisher.publishEvent(new BeforeSave( //
				Identifier.ofNullable(entityInformation.getId(instance)), //
				instance, //
				change //
		));

		change.executeWith(interpreter);

		publisher.publishEvent(new AfterSave( //
				Identifier.of(entityInformation.getId(instance)), //
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
		return accessStrategy.findById(id, domainType);
	}

	@Override
	public <T> boolean existsById(Object id, Class<T> domainType) {
		return accessStrategy.existsById(id, domainType);
	}

	@Override
	public <T> Iterable<T> findAll(Class<T> domainType) {
		return accessStrategy.findAll(domainType);
	}

	@Override
	public <T> Iterable<T> findAllById(Iterable<?> ids, Class<T> domainType) {
		return accessStrategy.findAllById(ids, domainType);
	}

	@Override
	public <S> void delete(S entity, Class<S> domainType) {

		JdbcPersistentEntityInformation<S, ?> entityInformation = context
				.getRequiredPersistentEntityInformation(domainType);
		deleteTree(entityInformation.getRequiredId(entity), entity, domainType);
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

	private void deleteTree(Object id, Object entity, Class<?> domainType) {

		AggregateChange change = createDeletingChange(id, entity, domainType);

		Specified specifiedId = Identifier.of(id);
		Optional<Object> optionalEntity = Optional.ofNullable(entity);
		publisher.publishEvent(new BeforeDelete(specifiedId, optionalEntity, change));

		change.executeWith(interpreter);

		publisher.publishEvent(new AfterDelete(specifiedId, optionalEntity, change));
	}

	private <T> AggregateChange createChange(T instance) {

		AggregateChange aggregateChange = new AggregateChange(Kind.SAVE, instance.getClass(), instance);
		jdbcEntityWriter.write(instance, aggregateChange);
		return aggregateChange;
	}

	private AggregateChange createDeletingChange(Object id, Object entity, Class<?> domainType) {

		AggregateChange aggregateChange = new AggregateChange(Kind.DELETE, domainType, entity);
		jdbcEntityDeleteWriter.write(id, aggregateChange);
		return aggregateChange;
	}

	private AggregateChange createDeletingChange(Class<?> domainType) {

		AggregateChange aggregateChange = new AggregateChange(Kind.DELETE, domainType, null);
		jdbcEntityDeleteWriter.write(null, aggregateChange);
		return aggregateChange;
	}
}
