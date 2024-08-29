/*
 * Copyright 2017-2024 the original author or authors.
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
import java.util.function.Consumer;

import org.springframework.data.convert.EntityWriter;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Converts an entity that is about to be deleted into {@link DbAction}s inside a {@link MutableAggregateChange} that
 * need to be executed against the database to recreate the appropriate state in the database. If the
 * {@link MutableAggregateChange} has a reference to the entity and the entity has a version attribute, the delete will
 * include an optimistic record locking check.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @author Bastian Wilhelm
 * @author Tyler Van Gorder
 * @author Myeonghyeon Lee
 * @author Chirag Tailor
 */
public class RelationalEntityDeleteWriter implements EntityWriter<Object, MutableAggregateChange<?>> {

	private final RelationalMappingContext context;

	public RelationalEntityDeleteWriter(RelationalMappingContext context) {

		Assert.notNull(context, "Context must not be null");

		this.context = context;
	}

	/**
	 * Fills the provided {@link MutableAggregateChange} with the necessary {@link DbAction}s to delete the aggregate root
	 * identified by {@code id}. If {@code id} is {@code null} it is interpreted as "Delete all aggregates of the type
	 * indicated by the aggregateChange".
	 *
	 * @param id may be {@code null}.
	 * @param aggregateChange must not be {@code null}.
	 */
	@Override
	public void write(@Nullable Object id, MutableAggregateChange<?> aggregateChange) {

		if (id == null) {
			deleteAll(aggregateChange.getEntityType()).forEach(aggregateChange::addAction);
		} else {
			deleteRoot(id, aggregateChange).forEach(aggregateChange::addAction);
		}
	}

	private List<DbAction<?>> deleteAll(Class<?> entityType) {

		List<DbAction<?>> deleteReferencedActions = new ArrayList<>();

		forAllTableRepresentingPaths(entityType, p -> deleteReferencedActions.add(new DbAction.DeleteAll<>(p)));

		Collections.reverse(deleteReferencedActions);

		List<DbAction<?>> actions = new ArrayList<>();
		if (!deleteReferencedActions.isEmpty()) {
			actions.add(new DbAction.AcquireLockAllRoot<>(entityType));
		}
		actions.addAll(deleteReferencedActions);

		DbAction.DeleteAllRoot<?> result = new DbAction.DeleteAllRoot<>(entityType);
		actions.add(result);

		return actions;
	}

	private <T> List<DbAction<?>> deleteRoot(Object id, MutableAggregateChange<T> aggregateChange) {

		List<DbAction<?>> deleteReferencedActions = deleteReferencedEntities(id, aggregateChange);

		List<DbAction<?>> actions = new ArrayList<>();
		if (!deleteReferencedActions.isEmpty()) {
			actions.add(new DbAction.AcquireLockRoot<>(id, aggregateChange.getEntityType()));
		}
		actions.addAll(deleteReferencedActions);

		actions.add(new DbAction.DeleteRoot<>(id, aggregateChange.getEntityType(), aggregateChange.getPreviousVersion()));

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

		forAllTableRepresentingPaths(aggregateChange.getEntityType(), p -> actions.add(new DbAction.Delete<>(id, p)));

		Collections.reverse(actions);

		return actions;
	}

	private void forAllTableRepresentingPaths(Class<?> entityType,
			Consumer<PersistentPropertyPath<RelationalPersistentProperty>> pathConsumer) {

		context.findPersistentPropertyPaths(entityType, property -> property.isEntity() && !property.isEmbedded()) //
				.filter(path -> context.getAggregatePath(path).isWritable()) //
				.forEach(pathConsumer);
	}
}
