/*
 * Copyright 2019-2022 the original author or authors.
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
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Visitable;

/**
 * {@link org.springframework.data.relational.core.sql.Visitor} rendering {@link Assignment}. Uses a
 * {@link RenderTarget} to call back for render results.
 *
 * @author Mark Paluch
 * @since 1.1
 * @see Assignment
 */
class AssignmentVisitor extends TypedSubtreeVisitor<Assignment> {

	private final ColumnVisitor columnVisitor;
	private final ExpressionVisitor expressionVisitor;
	private final RenderTarget target;
	private final StringBuilder part = new StringBuilder();

	AssignmentVisitor(RenderContext context, RenderTarget target) {
		this.columnVisitor = new ColumnVisitor(context, false, part::append);
		this.expressionVisitor = new ExpressionVisitor(context);
		this.target = target;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.FilteredSubtreeVisitor#enterNested(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation enterNested(Visitable segment) {

		if (segment instanceof Column) {
			return Delegation.delegateTo(columnVisitor);
		}

		if (segment instanceof Expression) {
			return Delegation.delegateTo(expressionVisitor);
		}

		throw new IllegalStateException("Cannot provide visitor for " + segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.FilteredSubtreeVisitor#leaveNested(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveNested(Visitable segment) {

		if (segment instanceof Column) {
			if (part.length() != 0) {
				part.append(" = ");
			}
			return super.leaveNested(segment);
		}

		if (segment instanceof Expression) {
			part.append(expressionVisitor.getRenderedPart());
		}

		return super.leaveNested(segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.FilteredSubtreeVisitor#leaveMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveMatched(Assignment segment) {

		target.onRendered(new StringBuilder(part));
		part.setLength(0);

		return super.leaveMatched(segment);
	}
}
