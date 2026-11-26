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
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link Upsert}.
 *
 * @author Christoph Strobl
 * @author YeongJae Min
 * @since 4.1
 */
record DefaultUpsert(Table table, List<Assignment> assignments, List<Column> conflictColumns,
		List<Column> updateColumns, @Nullable Condition updateCondition) implements Upsert {

	DefaultUpsert(Table table, List<Assignment> assignments, List<Column> conflictColumns, List<Column> updateColumns,
			@Nullable Condition updateCondition) {

		this.table = table;
		this.assignments = new ArrayList<>(assignments);
		this.conflictColumns = new ArrayList<>(conflictColumns);
		this.updateColumns = new ArrayList<>(updateColumns);
		this.updateCondition = updateCondition;
	}

	@Override
	public Table getTable() {
		return table();
	}

	@Override
	public List<Assignment> getAssignments() {
		return assignments();
	}

	@Override
	public List<Column> getConflictColumns() {
		return conflictColumns();
	}

	@Override
	public List<Column> getUpdateColumns() {
		return updateColumns();
	}

	@Override
	public @Nullable Condition getUpdateCondition() {
		return updateCondition();
	}

	@Override
	public void visit(Visitor visitor) {

		Assert.notNull(visitor, "Visitor must not be null");

		visitor.enter(this);

		this.table.visit(visitor);
		this.conflictColumns.forEach(col -> col.visit(visitor));
		this.assignments.forEach(it -> it.visit(visitor));
		this.updateColumns.forEach(col -> col.visit(visitor));
		if (this.updateCondition != null) {
			this.updateCondition.visit(visitor);
		}

		visitor.leave(this);
	}

	@Override
	public String toString() {

		if (this.updateCondition != null) {
			// Keep diagnostics independent of conditional-upsert dialect support.
			return "Upsert{table=" + this.table + ", assignments=" + this.assignments + ", conflictColumns="
					+ this.conflictColumns + ", updateColumns=" + this.updateColumns + ", updateCondition=" + this.updateCondition
					+ '}';
		}
		return SqlRenderer.toString(this);
	}

}
