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

import org.springframework.data.relational.core.sql.From;
import org.springframework.data.relational.core.sql.Join;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.SelectList;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.data.relational.core.sql.Where;

/**
 * {@link PartRenderer} for {@link Select} statements.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Myeonghyeon Lee
 * @since 1.1
 */
class SelectStatementVisitor extends DelegatingVisitor implements PartRenderer {

	private final RenderContext context;
	private final SelectRenderContext selectRenderContext;

	private StringBuilder builder = new StringBuilder();
	private StringBuilder selectList = new StringBuilder();
	private StringBuilder from = new StringBuilder();
	private StringBuilder join = new StringBuilder();
	private StringBuilder where = new StringBuilder();

	private SelectListVisitor selectListVisitor;
	private OrderByClauseVisitor orderByClauseVisitor;
	private FromClauseVisitor fromClauseVisitor;
	private WhereClauseVisitor whereClauseVisitor;

	SelectStatementVisitor(RenderContext context) {

		this.context = context;
		this.selectRenderContext = context.getSelectRenderContext();
		this.selectListVisitor = new SelectListVisitor(context, selectList::append);
		this.orderByClauseVisitor = new OrderByClauseVisitor(context);
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

		if (segment instanceof SelectList) {
			return Delegation.delegateTo(selectListVisitor);
		}

		if (segment instanceof OrderByField) {
			return Delegation.delegateTo(orderByClauseVisitor);
		}

		if (segment instanceof From) {
			return Delegation.delegateTo(fromClauseVisitor);
		}

		if (segment instanceof Join) {
			return Delegation.delegateTo(new JoinVisitor(context, it -> {

				if (join.length() != 0) {
					join.append(' ');
				}

				join.append(it);
			}));
		}

		if (segment instanceof Where) {
			return Delegation.delegateTo(whereClauseVisitor);
		}

		return Delegation.retain();
	}

	@Override
	public Delegation doLeave(Visitable segment) {

		if (segment instanceof Select select) {

			builder.append("SELECT ");

			if (select.isDistinct()) {
				builder.append("DISTINCT ");
			}

			builder.append(selectList);
			builder.append(selectRenderContext.afterSelectList().apply(select));

			if (from.length() != 0) {
				builder.append(" FROM ").append(from);
			}

			builder.append(selectRenderContext.afterFromTable().apply(select));

			if (join.length() != 0) {
				builder.append(' ').append(join);
			}

			if (where.length() != 0) {
				builder.append(" WHERE ").append(where);
			}

			CharSequence orderBy = orderByClauseVisitor.getRenderedPart();
			if (orderBy.length() != 0) {
				builder.append(" ORDER BY ").append(orderBy);
			}

			builder.append(selectRenderContext.afterOrderBy(orderBy.length() != 0).apply(select));

			return Delegation.leave();
		}

		return Delegation.retain();
	}

	@Override
	public CharSequence getRenderedPart() {
		return builder;
	}
}
