/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.relational.core.sql;

import java.util.List;
import java.util.function.Consumer;

import org.springframework.data.relational.core.sql.UpsertBuilder.UpsertInsert;

/**
 * An upsert (MERGE / INSERT … ON CONFLICT / INSERT … ON DUPLICATE KEY) statement.
 *
 * @author Christoph Strobl
 * @since 4.x
 */
public interface Upsert extends Segment, Visitable { // TODO: should we rename this to Merge?

	/**
	 * Start building an {@link Upsert} for the given {@link Table}.
	 *
	 * @param table the target table; must not be {@literal null}.
	 * @return the first builder step.
	 */
	static UpsertInsert builder(Table table) {
		return new DefaultUpsertBuilder(table);
	}

	/**
	 * Create an {@link Upsert} for the given {@link Table}.
	 *
	 * @param table the target table; must not be {@literal null}.
	 * @param consumer the consumer to configure the upsert.
	 * @return the first builder step.
	 */
	static Upsert create(Table table, Consumer<UpsertBuilder> consumer) {
		DefaultUpsertBuilder builder = new DefaultUpsertBuilder(table);
		consumer.accept(builder);
		return builder.build();
	}

	/** @return the target table. */
	Table getTable();

	/** @return the column-value assignments for the INSERT part. */
	List<Assignment> getAssignments();

	/** @return the columns that identify a conflicting row. */
	List<Column> getConflictColumns();
}
