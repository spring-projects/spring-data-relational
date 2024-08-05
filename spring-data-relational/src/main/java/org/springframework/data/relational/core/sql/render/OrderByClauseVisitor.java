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


import org.springframework.data.relational.core.sql.CaseExpression;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Expressions;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.SimpleFunction;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.lang.Nullable;

/**
 * {@link PartRenderer} for {@link OrderByField}s.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Chirag Tailor
 * @author Koen Punt
 * @author Sven Rienstra
 * @since 1.1
 */
class OrderByClauseVisitor extends TypedSubtreeVisitor<OrderByField> implements PartRenderer {

	private final RenderContext context;

	private final StringBuilder builder = new StringBuilder();

	@Nullable
	private PartRenderer delegate;

	private boolean first = true;

	OrderByClauseVisitor(RenderContext context) {
		this.context = context;
	}

	@Override
	Delegation enterMatched(OrderByField segment) {

		if (!first) {
			builder.append(", ");
		}
		first = false;

		return super.enterMatched(segment);
	}

	@Override
	Delegation leaveMatched(OrderByField segment) {

		if (segment.getDirection() != null) {

			builder.append(" ") //
					.append(segment.getDirection());
		}

		String nullPrecedence = context.getSelectRenderContext().evaluateOrderByNullHandling(segment.getNullHandling());
		if (!nullPrecedence.isEmpty()) {

			builder.append(" ") //
					.append(nullPrecedence);
		}

		return Delegation.leave();
	}

	@Override
	Delegation enterNested(Visitable segment) {

		if (segment instanceof SimpleFunction) {
			delegate = new SimpleFunctionVisitor(context);
			return Delegation.delegateTo((SimpleFunctionVisitor) delegate);
		}

		if (segment instanceof Expressions.SimpleExpression || segment instanceof CaseExpression) {
			delegate = new ExpressionVisitor(context);
			return Delegation.delegateTo((ExpressionVisitor) delegate);
		}

		return super.enterNested(segment);
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		if (delegate instanceof SimpleFunctionVisitor || delegate instanceof ExpressionVisitor) {
			builder.append(delegate.getRenderedPart());
			delegate = null;
		}

		if (segment instanceof Column) {
			builder.append(NameRenderer.fullyQualifiedReference(context, (Column) segment));
		}

		return super.leaveNested(segment);
	}

	@Override
	public CharSequence getRenderedPart() {
		return builder;
	}
}
