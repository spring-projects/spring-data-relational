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
package org.springframework.data.relational.core.conversion;

import lombok.Data;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.springframework.data.mapping.IdentifierAccessor;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.relational.core.conversion.DbAction.Insert;
import org.springframework.data.relational.core.conversion.DbAction.Update;
import org.springframework.data.relational.core.mapping.JdbcMappingContext;
import org.springframework.data.relational.core.mapping.JdbcPersistentEntity;
import org.springframework.data.relational.core.mapping.JdbcPersistentProperty;
import org.springframework.data.util.StreamUtils;

/**
 * Converts an entity that is about to be saved into {@link DbAction}s inside a {@link AggregateChange} that need to be
 * executed against the database to recreate the appropriate state in the database.
 *
 * @author Jens Schauder
 * @since 1.0
 */
public class JdbcEntityWriter extends JdbcEntityWriterSupport {

	private final JdbcMappingContext context;

	public JdbcEntityWriter(JdbcMappingContext context) {

		super(context);

		this.context = context;
	}

	/**
	 * Converts an aggregate represented by its aggregate root into a list of {@link DbAction}s and adds them to the
	 * {@link AggregateChange} passed in as an argument.
	 * 
	 * @param aggregateRoot the aggregate root to be written to the {@link AggregateChange} Must not be {@code null}.
	 * @param aggregateChange the {@link AggregateChange} to which the {@link DbAction}s get written.
	 */
	@Override
	public void write(Object aggregateRoot, AggregateChange aggregateChange) {

		Class<?> type = aggregateRoot.getClass();
		JdbcPropertyPath propertyPath = JdbcPropertyPath.from("", type);

		PersistentEntity<?, JdbcPersistentProperty> persistentEntity = context.getRequiredPersistentEntity(type);

		if (persistentEntity.isNew(aggregateRoot)) {

			Insert<Object> insert = DbAction.insert(aggregateRoot, propertyPath, null);
			aggregateChange.addAction(insert);

			referencedEntities(aggregateRoot) //
					.forEach( //
							propertyAndValue -> //
							insertReferencedEntities( //
									propertyAndValue, //
									aggregateChange, //
									propertyPath.nested(propertyAndValue.property.getName()), //
									insert) //
			);
		} else {

			JdbcPersistentEntity<?> entity = context.getRequiredPersistentEntity(type);
			IdentifierAccessor identifierAccessor = entity.getIdentifierAccessor(aggregateRoot);

			deleteReferencedEntities(identifierAccessor.getRequiredIdentifier(), aggregateChange);

			Update<Object> update = DbAction.update(aggregateRoot, propertyPath, null);
			aggregateChange.addAction(update);

			referencedEntities(aggregateRoot).forEach(propertyAndValue -> insertReferencedEntities(propertyAndValue,
					aggregateChange, propertyPath.nested(propertyAndValue.property.getName()), update));
		}
	}

	private void insertReferencedEntities(PropertyAndValue propertyAndValue, AggregateChange aggregateChange,
			JdbcPropertyPath propertyPath, DbAction dependingOn) {

		Insert<Object> insert;
		if (propertyAndValue.property.isQualified()) {

			KeyValue valueAsEntry = (KeyValue) propertyAndValue.value;
			insert = DbAction.insert(valueAsEntry.getValue(), propertyPath, dependingOn);
			insert.getAdditionalValues().put(propertyAndValue.property.getKeyColumn(), valueAsEntry.getKey());
		} else {
			insert = DbAction.insert(propertyAndValue.value, propertyPath, dependingOn);
		}

		aggregateChange.addAction(insert);
		referencedEntities(insert.getEntity()) //
				.forEach(pav -> insertReferencedEntities( //
						pav, //
						aggregateChange, //
						propertyPath.nested(pav.property.getName()), //
						dependingOn) //
		);
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

		if (List.class.isAssignableFrom(type)) {
			return listPropertyAsStream(p, propertyAccessor);
		}

		if (Collection.class.isAssignableFrom(type)) {
			return collectionPropertyAsStream(p, propertyAccessor);
		}

		if (Map.class.isAssignableFrom(type)) {
			return mapPropertyAsStream(p, propertyAccessor);
		}

		return singlePropertyAsStream(p, propertyAccessor);
	}

	@SuppressWarnings("unchecked")
	private Stream<Object> collectionPropertyAsStream(JdbcPersistentProperty p,
			PersistentPropertyAccessor propertyAccessor) {

		Object property = propertyAccessor.getProperty(p);

		return property == null //
				? Stream.empty() //
				: ((Collection<Object>) property).stream();
	}

	@SuppressWarnings("unchecked")
	private Stream<Object> listPropertyAsStream(JdbcPersistentProperty p, PersistentPropertyAccessor propertyAccessor) {

		Object property = propertyAccessor.getProperty(p);

		if (property == null) {
			return Stream.empty();
		}

		// ugly hackery since Java streams don't have a zip method.
		AtomicInteger index = new AtomicInteger();
		List<Object> listProperty = (List<Object>) property;

		return listProperty.stream().map(e -> new KeyValue(index.getAndIncrement(), e));
	}

	@SuppressWarnings("unchecked")
	private Stream<Object> mapPropertyAsStream(JdbcPersistentProperty p, PersistentPropertyAccessor propertyAccessor) {

		Object property = propertyAccessor.getProperty(p);

		return property == null //
				? Stream.empty() //
				: ((Map<Object, Object>) property).entrySet().stream().map(e -> new KeyValue(e.getKey(), e.getValue()));
	}

	private Stream<Object> singlePropertyAsStream(JdbcPersistentProperty p, PersistentPropertyAccessor propertyAccessor) {

		Object property = propertyAccessor.getProperty(p);
		if (property == null) {
			return Stream.empty();
		}

		return Stream.of(property);
	}

	/**
	 * Holds key and value of a {@link Map.Entry} but without any ties to {@link Map} implementations.
	 */
	@Data
	private static class KeyValue {
		private final Object key;
		private final Object value;
	}

	@Data
	private static class PropertyAndValue {

		private final JdbcPersistentProperty property;
		private final Object value;
	}
}
