/*
 * Copyright 2019-2025 the original author or authors.
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

import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.OperatorExpression;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.lang.Nullable;

/**
 * {@link org.springframework.data.relational.core.sql.Visitor} rendering comparison {@link OperatorExpression}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 * @see OperatorExpression
 */
class OperatorExpressionVisitor extends FilteredSubtreeVisitor implements PartRenderer {

	private final RenderContext context;
	private final OperatorExpression expression;
	private @Nullable final RenderTarget target;
	private final StringBuilder part = new StringBuilder();
	private @Nullable PartRenderer current;

	OperatorExpressionVisitor(RenderContext context, OperatorExpression expression) {

		super(it -> it == expression);

		this.expression = expression;
		this.target = null;
		this.context = context;
	}

	OperatorExpressionVisitor(RenderContext context, OperatorExpression expression, RenderTarget target) {

		super(it -> it == expression);

		this.expression = expression;
		this.target = target;
		this.context = context;
	}

	@Override
	Delegation enterNested(Visitable segment) {

		if (segment instanceof Expression) {
			ExpressionVisitor visitor = new ExpressionVisitor(context);
			current = visitor;
			return Delegation.delegateTo(visitor);
		}

		throw new IllegalStateException("Cannot provide visitor for " + segment);
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		if (current != null) {
			if (!part.isEmpty()) {
				part.append(' ').append(expression.getOperator()).append(' ');
			}

			part.append(current.getRenderedPart());
			current = null;
		}

		return super.leaveNested(segment);
	}

	@Override
	Delegation leaveMatched(Visitable segment) {

		if (target != null) {
			target.onRendered(part);
		}

		return super.leaveMatched(segment);
	}

	@Override
	public CharSequence getRenderedPart() {

		String part = this.part.toString();
		this.part.setLength(0);

		return part;
	}
}
