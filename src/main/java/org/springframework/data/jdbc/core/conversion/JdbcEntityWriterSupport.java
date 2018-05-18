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
package org.springframework.data.jdbc.core.conversion;

import org.springframework.data.convert.EntityWriter;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;

/**
 * Common infrastructure needed by different implementations of {@link EntityWriter}<Object, AggregateChange>.
 *
 * @author Jens Schauder
 * @since 1.0
 */
abstract class JdbcEntityWriterSupport implements EntityWriter<Object, AggregateChange> {
	protected final JdbcMappingContext context;

	JdbcEntityWriterSupport(JdbcMappingContext context) {
		this.context = context;
	}

	/**
	 * add {@link org.springframework.data.jdbc.core.conversion.DbAction.Delete} actions to the {@link AggregateChange} for
	 * deleting all referenced entities.
	 *
	 * @param id id of the aggregate root, of which the referenced entities get deleted.
	 * @param aggregateChange the change object to which the actions should get added. Must not be {@literal null}
	 */
	void deleteReferencedEntities(Object id, AggregateChange aggregateChange) {

		context.referencedEntities(aggregateChange.getEntityType(), null)
				.forEach(p -> aggregateChange.addAction(DbAction.delete(id, p.getLeafType(), null, new JdbcPropertyPath(p), null)));
	}
}
