/*
 * Copyright 2019-2024 the original author or authors.
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

import org.springframework.data.relational.core.sql.Delete;
import org.springframework.data.relational.core.sql.From;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.data.relational.core.sql.Where;

/**
 * {@link PartRenderer} for {@link Delete} statements.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class DeleteStatementVisitor extends DelegatingVisitor implements PartRenderer {

	private StringBuilder builder = new StringBuilder();
	private StringBuilder from = new StringBuilder();
	private StringBuilder where = new StringBuilder();

	private FromClauseVisitor fromClauseVisitor;
	private WhereClauseVisitor whereClauseVisitor;

	DeleteStatementVisitor(RenderContext context) {

		this.fromClauseVisitor = new FromClauseVisitor(context, it -> {

			if (from.length() != 0) {
				from.append(", ");
			}

			from.append(it);
		});

		this.whereClauseVisitor = new WhereClauseVisitor(context, where::append);
	}

	@Override
	public Delegation doEnter(Visitable segment) {

		if (segment instanceof From) {
			return Delegation.delegateTo(fromClauseVisitor);
		}

		if (segment instanceof Where) {
			return Delegation.delegateTo(whereClauseVisitor);
		}

		return Delegation.retain();
	}

	@Override
	public Delegation doLeave(Visitable segment) {

		if (segment instanceof Delete) {

			builder.append("DELETE ");

			if (from.length() != 0) {
				builder.append("FROM ").append(from);
			}

			if (where.length() != 0) {
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
