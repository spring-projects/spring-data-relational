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
package org.springframework.data.jdbc.core.conversion;

import java.util.stream.Stream;

import org.springframework.data.jdbc.core.conversion.DbAction.Insert;
import org.springframework.data.jdbc.core.conversion.DbAction.Update;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntity;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntityInformation;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentProperty;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.util.StreamUtils;

/**
 * Converts an entity that is about to be saved into {@link DbAction}s inside a {@link AggregateChange} that need to be
 * executed against the database to recreate the appropriate state in the database.
 *
 * @author Jens Schauder
 */
public class JdbcEntityWriter extends JdbcEntityWriterSupport {

	public JdbcEntityWriter(JdbcMappingContext context) {
		super(context);
	}

	@Override
	public void write(Object o, AggregateChange aggregateChange) {
		write(o, aggregateChange, null);
	}

	private void write(Object o, AggregateChange aggregateChange, DbAction dependingOn) {

		JdbcPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(aggregateChange.getEntityType());

		JdbcPersistentEntityInformation<Object, ?> entityInformation = context
				.getRequiredPersistentEntityInformation((Class<Object>) o.getClass());

		if (entityInformation.isNew(o)) {

			Insert<Object> insert = DbAction.insert(o, dependingOn);
			aggregateChange.addAction(insert);

			referencedEntities(o).forEach(e -> saveReferencedEntities(e, aggregateChange, insert));
		} else {

			deleteReferencedEntities(entityInformation.getRequiredId(o), aggregateChange);

			Update<Object> update = DbAction.update(o, dependingOn);
			aggregateChange.addAction(update);

			referencedEntities(o).forEach(e -> insertReferencedEntities(e, aggregateChange, update));
		}
	}

	private void saveReferencedEntities(Object o, AggregateChange aggregateChange, DbAction dependingOn) {

		DbAction action = saveAction(o, dependingOn);
		aggregateChange.addAction(action);

		referencedEntities(o).forEach(e -> saveReferencedEntities(e, aggregateChange, action));
	}

	private <T> DbAction saveAction(T t, DbAction dependingOn) {

		JdbcPersistentEntityInformation<T, ?> entityInformation = context
				.getRequiredPersistentEntityInformation((Class<T>) t.getClass());

		if (entityInformation.isNew(t)) {
			return DbAction.insert(t, dependingOn);
		} else {
			return DbAction.update(t, dependingOn);
		}
	}

	private void insertReferencedEntities(Object o, AggregateChange aggregateChange, DbAction dependingOn) {

		aggregateChange.addAction(DbAction.insert(o, dependingOn));
		referencedEntities(o).forEach(e -> insertReferencedEntities(e, aggregateChange, dependingOn));
	}

	private Stream<Object> referencedEntities(Object o) {

		JdbcPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(o.getClass());

		return StreamUtils.createStreamFromIterator(persistentEntity.iterator()) //
				.filter(PersistentProperty::isEntity)
				.flatMap(p -> referencedEntity(p, persistentEntity.getPropertyAccessor(o)));
	}

	private Stream<?> referencedEntity(JdbcPersistentProperty p, PersistentPropertyAccessor propertyAccessor) {

		Class<?> type = p.getActualType();
		JdbcPersistentEntity<?> persistentEntity = context //
				.getPersistentEntity(type);

		if (persistentEntity == null) {
			return Stream.empty();
		}

		Object property = propertyAccessor.getProperty(p);
		if (property == null) {
			return Stream.empty();
		}

		return Stream.of(property);
	}
}
