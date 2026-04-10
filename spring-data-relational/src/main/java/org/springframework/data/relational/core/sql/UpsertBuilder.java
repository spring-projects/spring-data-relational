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

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * Fluent builder for {@link Upsert} statements.
 * <p>
 * Usage:
 *
 * <pre class="code">
 *     StatementBuilder.upsert(table).insert(col.set(marker), …).onConflict(idCol).update();
 * </pre>
 *
 * @author Christoph Strobl
 * @since 4.1
 * @see StatementBuilder
 */
public interface UpsertBuilder {

	/**
	 * Step for specifying the columns and values to insert.
	 */
	interface UpsertInsert {

		/**
		 * Specify the column-value assignments for the insert.
		 *
		 * @param assignments one or more {@link Assignment column assignments}; must not be {@literal null}.
		 * @return the next builder step.
		 */
		default UpsertOnMatch insert(Assignment... assignments) {
			return insert(List.of(assignments));
		}

		/**
		 * Specify the column-value assignments for the insert.
		 *
		 * @param assignments the {@link Assignment column assignments}; must not be {@literal null}.
		 * @return the next builder step.
		 */
		UpsertOnMatch insert(Collection<? extends Assignment> assignments);

	}

	/**
	 * Step for specifying the conflict target columns.
	 */
	interface UpsertOnMatch {

		/**
		 * Declare how to resolve a conflict if the row already exists.
		 *
		 * @param resolution the conflict columns; must not be {@literal null}.
		 * @return the terminal build step.
		 */
		BuildUpsert onConflict(Function<ConflictColumn, ConflictResolution> resolution);

	}

	/**
	 * Step for specifying what to do when a conflict is detected.
	 */
	interface ConflictColumn {

		/**
		 * Start building the conflict resolution by specifying a conflict/matching column.
		 *
		 * @param column the column to specify conflict resolution; must not be {@literal null}.
		 * @return the terminal build step.
		 */
		default OngoingConflictResolution with(Column column) {
			return with(List.of(column));
		}

		/**
		 * Start building the conflict resolution by specifying conflict/matching columns.
		 *
		 * @param columns the columns to specify conflict resolution; must not be {@literal null}.
		 * @return the terminal build step.
		 */
		default OngoingConflictResolution with(Column... columns) {
			return with(List.of(columns));
		}

		/**
		 * Start building the conflict resolution by specifying conflict/matching columns.
		 *
		 * @param columns the columns to specify conflict resolution; must not be {@literal null}.
		 * @return the terminal build step.
		 */
		OngoingConflictResolution with(Collection<? extends Column> columns);

	}

	/**
	 * Step for specifying what to do when a conflict is detected.
	 */
	interface OngoingConflictResolution {

		/**
		 * Resolve the conflict by updating the existing row using the previously specified assignments for non-conflicting
		 * columns that are not part of {@link ConflictColumn#with}.
		 *
		 * @return the conflict resolution strategy.
		 */
		ConflictResolution updateRemainingColumns();

		/**
		 * Resolve the conflict by updating the given {@link Column columns} of the existing row.
		 *
		 * @return the conflict resolution strategy.
		 */
		default ConflictResolution update(Column... columns) {
			return update(List.of(columns));
		}

		/**
		 * Resolve the conflict by updating the given {@link Column columns} of the existing row.
		 *
		 * @return the conflict resolution strategy.
		 */
		ConflictResolution update(Collection<Column> columns);
	}

	/**
	 * The actual conflict resolution strategy.
	 */
	interface ConflictResolution {

	}

	/**
	 * Terminal step that produces the {@link Upsert} statement.
	 */
	interface BuildUpsert {

		/**
		 * Build the immutable {@link Upsert} statement.
		 *
		 * @return the {@link Upsert} statement.
		 */
		Upsert build();

	}

}
