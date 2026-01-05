/*
 * Copyright 2019-present the original author or authors.
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
package org.springframework.data.relational.core.sql.render;

import org.springframework.data.relational.core.sql.Assignment;
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.Update;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.data.relational.core.sql.Where;

/**
 * {@link PartRenderer} for {@link Update} statements.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class UpdateStatementVisitor extends DelegatingVisitor implements PartRenderer {

	private final StringBuilder builder = new StringBuilder();
	private final StringBuilder table = new StringBuilder();
	private final StringBuilder assignments = new StringBuilder();
	private final StringBuilder where = new StringBuilder();

	private final FromTableVisitor tableVisitor;
	private final AssignmentVisitor assignmentVisitor;
	private final WhereClauseVisitor whereClauseVisitor;

	UpdateStatementVisitor(RenderContext context) {

		this.tableVisitor = new FromTableVisitor(context, it -> {

			if (!table.isEmpty()) {
				table.append(", ");
			}

			table.append(it);
		});

		this.assignmentVisitor = new AssignmentVisitor(context, it -> {

			if (!assignments.isEmpty()) {
				assignments.append(", ");
			}

			assignments.append(it);
		});

		this.whereClauseVisitor = new WhereClauseVisitor(context, where::append);
	}

	@Override
	public Delegation doEnter(Visitable segment) {

		if (segment instanceof Table) {
			return Delegation.delegateTo(this.tableVisitor);
		}

		if (segment instanceof Assignment) {
			return Delegation.delegateTo(this.assignmentVisitor);
		}

		if (segment instanceof Where) {
			return Delegation.delegateTo(this.whereClauseVisitor);
		}

		return Delegation.retain();
	}

	@Override
	public Delegation doLeave(Visitable segment) {

		if (segment instanceof Update) {

			builder.append("UPDATE");

			if (!table.isEmpty()) {
				builder.append(" ").append(table);
			}

			if (!assignments.isEmpty()) {
				builder.append(" SET ").append(assignments);
			}

			if (!where.isEmpty()) {
				builder.append(" WHERE ").append(where);
			}

			return Delegation.leave();
		}

		return Delegation.retain();
	}

	@Override
	public CharSequence getRenderedPart() {
		return builder;
	}
}
