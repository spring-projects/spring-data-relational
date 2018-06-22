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

import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.lang.Nullable;

/**
 * Converts an entity that is about to be deleted into {@link DbAction}s inside a {@link AggregateChange} that need to
 * be executed against the database to recreate the appropriate state in the database.
 *
 * @author Jens Schauder
 * @since 1.0
 */
public class RelationalEntityDeleteWriter extends RelationalEntityWriterSupport {

	public RelationalEntityDeleteWriter(RelationalMappingContext context) {
		super(context);
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
			deleteAll(aggregateChange);
		} else {
			deleteById(id, aggregateChange);
		}
	}

	private void deleteAll(AggregateChange<?> aggregateChange) {

		context.referencedEntities(aggregateChange.getEntityType(), null)
				.forEach(p -> aggregateChange.addAction(DbAction.deleteAll(p.getLeafType(), new RelationalPropertyPath(p), null)));

		aggregateChange.addAction(DbAction.deleteAll(aggregateChange.getEntityType(), null, null));
	}

	private <T> void deleteById(Object id, AggregateChange<T> aggregateChange) {

		deleteReferencedEntities(id, aggregateChange);

		aggregateChange.addAction(DbAction.delete( //
				id, //
				aggregateChange.getEntityType(), //
				aggregateChange.getEntity(), //
				null, //
				null //
		));
	}
}
