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

import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
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
import org.springframework.util.ClassUtils;

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

		Class<Object> type = (Class<Object>) o.getClass();
		JdbcPersistentEntityInformation<Object, ?> entityInformation = context.getRequiredPersistentEntityInformation(type);
		JdbcPropertyPath propertyPath = JdbcPropertyPath.from("", type);

		if (entityInformation.isNew(o)) {

			Insert<Object> insert = DbAction.insert(o, propertyPath, dependingOn);
			aggregateChange.addAction(insert);

			referencedEntities(o).forEach(propertyAndValue -> saveReferencedEntities(propertyAndValue, aggregateChange,
					propertyPath.nested(propertyAndValue.property.getName()), insert));
		} else {

			deleteReferencedEntities(entityInformation.getRequiredId(o), aggregateChange);

			Update<Object> update = DbAction.update(o, propertyPath, dependingOn);
			aggregateChange.addAction(update);

			referencedEntities(o).forEach(
					propertyAndValue -> insertReferencedEntities(propertyAndValue, aggregateChange, propertyPath.nested(propertyAndValue.property.getName()), update));
		}
	}

	private void saveReferencedEntities(PropertyAndValue propertyAndValue, AggregateChange aggregateChange,
			JdbcPropertyPath propertyPath, DbAction dependingOn) {

		saveActions(propertyAndValue, propertyPath, dependingOn).forEach(a -> {

			aggregateChange.addAction(a);
			referencedEntities(propertyAndValue.value)
					.forEach(pav -> saveReferencedEntities(pav, aggregateChange, propertyPath.nested(pav.property.getName()), a));
		});
	}

	private Stream<DbAction> saveActions(PropertyAndValue propertyAndValue, JdbcPropertyPath propertyPath,
			DbAction dependingOn) {

		if (Map.Entry.class.isAssignableFrom(ClassUtils.getUserClass(propertyAndValue.value))) {
			return mapEntrySaveAction(propertyAndValue, propertyPath, dependingOn);
		}

		return Stream.of(singleSaveAction(propertyAndValue.value, propertyPath, dependingOn));
	}

	private Stream<DbAction> mapEntrySaveAction(PropertyAndValue propertyAndValue, JdbcPropertyPath propertyPath,
			DbAction dependingOn) {

		Map.Entry<Object, Object> entry = (Map.Entry) propertyAndValue.value;

		DbAction action = singleSaveAction(entry.getValue(), propertyPath, dependingOn);
		action.getAdditionalValues().put(propertyAndValue.property.getKeyColumn(), entry.getKey());
		return Stream.of(action);
	}

	private <T> DbAction singleSaveAction(T t, JdbcPropertyPath propertyPath, DbAction dependingOn) {

		JdbcPersistentEntityInformation<T, ?> entityInformation = context
				.getRequiredPersistentEntityInformation((Class<T>) ClassUtils.getUserClass(t));

		return entityInformation.isNew(t) ? DbAction.insert(t, propertyPath, dependingOn)
				: DbAction.update(t, propertyPath, dependingOn);
	}

	private void insertReferencedEntities(PropertyAndValue propertyAndValue, AggregateChange aggregateChange,
			JdbcPropertyPath propertyPath, DbAction dependingOn) {

		Insert<Object> insert;
		if (propertyAndValue.property.isQualified()) {

			Entry<Object, Object> valueAsEntry = (Entry<Object, Object>) propertyAndValue.value;
			insert = DbAction.insert(valueAsEntry.getValue(), propertyPath, dependingOn);
			insert.getAdditionalValues().put(propertyAndValue.property.getKeyColumn(), valueAsEntry.getKey());
		} else {
			insert = DbAction.insert(propertyAndValue.value, propertyPath, dependingOn);
		}

		aggregateChange.addAction(insert);
		referencedEntities(insert.getEntity())
				.forEach(pav -> insertReferencedEntities(pav, aggregateChange, propertyPath.nested(pav.property.getName()), dependingOn));
	}

	private Stream<PropertyAndValue> referencedEntities(Object o) {

		JdbcPersistentEntity<?> persistentEntity = context.getRequiredPersistentEntity(o.getClass());

		return StreamUtils.createStreamFromIterator(persistentEntity.iterator()) //
				.filter(PersistentProperty::isEntity) //
				.flatMap( //
						p -> referencedEntity(p, persistentEntity.getPropertyAccessor(o)) //
								.map(e -> new PropertyAndValue(p, e)) //
		);
	}

	private Stream<Object> referencedEntity(JdbcPersistentProperty p, PersistentPropertyAccessor propertyAccessor) {

		Class<?> actualType = p.getActualType();
		JdbcPersistentEntity<?> persistentEntity = context //
				.getPersistentEntity(actualType);

		if (persistentEntity == null) {
			return Stream.empty();
		}

		Class<?> type = p.getType();

		if (Collection.class.isAssignableFrom(type)) {
			return collectionPropertyAsStream(p, propertyAccessor);
		}

		if (Map.class.isAssignableFrom(type)) {
			return mapPropertyAsStream(p, propertyAccessor);
		}

		return singlePropertyAsStream(p, propertyAccessor);
	}

	private Stream<Object> collectionPropertyAsStream(JdbcPersistentProperty p,
			PersistentPropertyAccessor propertyAccessor) {

		Object property = propertyAccessor.getProperty(p);

		return property == null //
				? Stream.empty() //
				: ((Collection<Object>) property).stream();
	}

	private Stream<Object> mapPropertyAsStream(JdbcPersistentProperty p, PersistentPropertyAccessor propertyAccessor) {

		Object property = propertyAccessor.getProperty(p);

		return property == null //
				? Stream.empty() //
				: ((Map<Object, Object>) property).entrySet().stream().map(e -> (Object) e);
	}

	private Stream<Object> singlePropertyAsStream(JdbcPersistentProperty p, PersistentPropertyAccessor propertyAccessor) {

		Object property = propertyAccessor.getProperty(p);
		if (property == null) {
			return Stream.empty();
		}

		return Stream.of(property);
	}

	@RequiredArgsConstructor
	private static class PropertyAndValue {

		private final JdbcPersistentProperty property;
		private final Object value;
	}
}
