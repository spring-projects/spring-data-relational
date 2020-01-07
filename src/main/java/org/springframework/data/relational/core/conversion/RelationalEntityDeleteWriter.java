/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.relational.core.conversion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.data.convert.EntityWriter;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Converts an entity that is about to be deleted into {@link DbAction}s inside a {@link AggregateChange} that need to
 * be executed against the database to recreate the appropriate state in the database.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
public class RelationalEntityDeleteWriter implements EntityWriter<Object, AggregateChange<?>> {

	private final RelationalMappingContext context;

	public RelationalEntityDeleteWriter(RelationalMappingContext context) {

		Assert.notNull(context, "Context must not be null");

		this.context = context;
	}

	/**
	 * Fills the provided {@link AggregateChange} with the necessary {@link DbAction}s to delete the aggregate root
	 * identified by {@code id}. If {@code id} is {@code null} it is interpreted as "Delete all aggregates of the type
	 * indicated by the aggregateChange".
	 *
	 * @param id May be {@code null}.
	 * @param aggregateChange Must not be {@code null}.
	 */
	@Override
	public void write(@Nullable Object id, AggregateChange<?> aggregateChange) {

		if (id == null) {
			deleteAll(aggregateChange.getEntityType()).forEach(aggregateChange::addAction);
		} else {
			deleteById(id, aggregateChange).forEach(aggregateChange::addAction);
		}
	}

	private List<DbAction<?>> deleteAll(Class<?> entityType) {

		List<DbAction<?>> actions = new ArrayList<>();

		context.findPersistentPropertyPaths(entityType, PersistentProperty::isEntity)
				.forEach(p -> actions.add(new DbAction.DeleteAll<>(p)));

		Collections.reverse(actions);

		DbAction.DeleteAllRoot<?> result = new DbAction.DeleteAllRoot<>(entityType);
		actions.add(result);

		return actions;
	}

	private <T> List<DbAction<?>> deleteById(Object id, AggregateChange<T> aggregateChange) {

		List<DbAction<?>> actions = new ArrayList<>(deleteReferencedEntities(id, aggregateChange));
		actions.add(new DbAction.DeleteRoot<>(aggregateChange.getEntityType(), id));

		return actions;
	}

	/**
	 * Add {@link DbAction.Delete} actions to the {@link AggregateChange} for deleting all referenced entities.
	 *
	 * @param id id of the aggregate root, of which the referenced entities get deleted.
	 * @param aggregateChange the change object to which the actions should get added. Must not be {@code null}
	 */
	private List<DbAction<?>> deleteReferencedEntities(Object id, AggregateChange<?> aggregateChange) {

		List<DbAction<?>> actions = new ArrayList<>();

		context.findPersistentPropertyPaths(aggregateChange.getEntityType(), PersistentProperty::isEntity)
				.forEach(p -> actions.add(new DbAction.Delete<>(id, p)));

		Collections.reverse(actions);

		return actions;
	}
}
