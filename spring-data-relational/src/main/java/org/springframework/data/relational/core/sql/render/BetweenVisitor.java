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

import org.springframework.data.relational.core.sql.Between;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.lang.Nullable;

/**
 * {@link org.springframework.data.relational.core.sql.Visitor} rendering comparison {@link Condition}. Uses a
 * {@link RenderTarget} to call back for render results.
 *
 * @author Mark Paluch
 * @see Between
 * @since 2.0
 */
class BetweenVisitor extends FilteredSubtreeVisitor {

	private final Between between;
	private final RenderContext context;
	private final RenderTarget target;
	private final StringBuilder part = new StringBuilder();
	private boolean renderedTestExpression = false;
	private boolean renderedPreamble = false;
	private boolean done = false;
	private @Nullable PartRenderer current;

	BetweenVisitor(Between condition, RenderContext context, RenderTarget target) {
		super(it -> it == condition);
		this.between = condition;
		this.context = context;
		this.target = target;
	}

	@Override
	Delegation enterNested(Visitable segment) {

		if (segment instanceof Expression) {
			ExpressionVisitor visitor = new ExpressionVisitor(context);
			current = visitor;
			return Delegation.delegateTo(visitor);
		}

		if (segment instanceof Condition) {
			ConditionVisitor visitor = new ConditionVisitor(context);
			current = visitor;
			return Delegation.delegateTo(visitor);
		}

		throw new IllegalStateException("Cannot provide visitor for " + segment);
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		if (current != null && !done) {

			if (renderedPreamble) {

				part.append(" AND ");
				part.append(current.getRenderedPart());
				done = true;
			}

			if (renderedTestExpression && !renderedPreamble) {

				part.append(' ');

				if (between.isNegated()) {
					part.append("NOT ");
				}

				part.append("BETWEEN ");
				renderedPreamble = true;
				part.append(current.getRenderedPart());
			}

			if (!renderedTestExpression) {
				part.append(current.getRenderedPart());
				renderedTestExpression = true;
			}

			current = null;
		}

		return super.leaveNested(segment);
	}

	@Override
	Delegation leaveMatched(Visitable segment) {

		target.onRendered(part);

		return super.leaveMatched(segment);
	}
}
