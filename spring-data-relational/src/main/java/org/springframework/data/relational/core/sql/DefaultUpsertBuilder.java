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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.springframework.data.relational.core.sql.UpsertBuilder.BuildUpsert;
import org.springframework.data.relational.core.sql.UpsertBuilder.UpsertInsert;
import org.springframework.data.relational.core.sql.UpsertBuilder.UpsertOnMatch;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Default {@link UpsertBuilder} implementation.
 *
 * @author Christoph Strobl
 * @since 4.1
 */
class DefaultUpsertBuilder
		implements UpsertBuilder, UpsertInsert, UpsertOnMatch, BuildUpsert, UpsertBuilder.ConflictResolution {

	private final Table table;
	private final List<Assignment> assignments = new ArrayList<>();
	private final List<Column> conflictColumns = new ArrayList<>();

	DefaultUpsertBuilder(Table table) {

		Assert.notNull(table, "Table must not be null");
		this.table = table;
	}

	@Override
	public UpsertOnMatch insert(Collection<? extends Assignment> assignments) {

		Assert.notNull(assignments, "Assignments must not be null");
		this.assignments.addAll(assignments);
		return this;
	}

	@Override
	public BuildUpsert onConflict(Function<ConflictColumn, ConflictResolution> resolution) {

		Assert.notNull(resolution, "ConflictResolution Consumer must not be null");

		ConflictColumn conflictResolution = columns -> (OngoingConflictResolution) () -> {
			conflictColumns.addAll(columns);
			return DefaultUpsertBuilder.this;
		};

		resolution.apply(conflictResolution);
		return this;
	}

	@Override
	public Upsert build() {
		validate();
		return new DefaultUpsert(this.table, this.assignments, this.conflictColumns);
	}

	void validate() {

		Assert.state(!this.conflictColumns.isEmpty(), "Conflict columns must not be empty");

		for (Column column : this.conflictColumns) {

			boolean present = this.assignments.stream().map(it -> {
				if (it instanceof AssignValue av) {
					return av.getColumn();
				}
				return null;
			}).anyMatch(it -> ObjectUtils.nullSafeEquals(column.getName().getReference(),
					it != null ? it.getName().getReference() : null));

			if (!present) {
				throw new IllegalStateException("No value for conflict column [%s]".formatted(column.getName().getReference()));
			}
		}
	}
}
