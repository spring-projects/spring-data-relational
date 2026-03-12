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

/**
 * Fluent builder for {@link Upsert} statements.
 * <p>
 * Usage: {@code StatementBuilder.upsert(table).insert(col.set(marker), …).onConflict(idCol).update()}
 *
 * @author Christoph Strobl
 * @since 4.x
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
		default UpsertOnConflict insert(Assignment... assignments) {
			return insert(List.of(assignments));
		}

		/**
		 * Specify the column-value assignments for the insert.
		 *
		 * @param assignments the {@link Assignment column assignments}; must not be {@literal null}.
		 * @return the next builder step.
		 */
		UpsertOnConflict insert(Collection<? extends Assignment> assignments);
	}

	/**
	 * Step for specifying the conflict target columns.
	 */
	interface UpsertOnConflict {

		/**
		 * Declare the columns whose uniqueness constraint drives conflict detection.
		 *
		 * @param columns one or more conflict columns; must not be {@literal null}.
		 * @return the terminal build step.
		 */
		default UpsertResolution onConflict(Column... columns) {
			return onConflict(List.of(columns));
		}

		/**
		 * Declare the columns whose uniqueness constraint drives conflict detection.
		 *
		 * @param columns the conflict columns; must not be {@literal null}.
		 * @return the terminal build step.
		 */
		UpsertResolution onConflict(Collection<? extends Column> columns);
	}

	/**
	 * Step for specifying what to do when a conflict is detected.
	 */
	interface UpsertResolution {
		BuildUpsert update();
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
