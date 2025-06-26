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

import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.NestedExpression;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.lang.Nullable;

/**
 * Renderer for {@link NestedExpression}. Can use a {@link RenderTarget} to call back for render results.
 *
 * @author Mark Paluch
 * @since 4.0
 */
class NestedExpressionVisitor extends TypedSubtreeVisitor<NestedExpression> implements PartRenderer {

	private final RenderContext context;
	private final @Nullable RenderTarget target;

	private @Nullable ConditionVisitor conditionVisitor;
	private @Nullable ExpressionVisitor expressionVisitor;
	private final StringBuilder builder = new StringBuilder();

	NestedExpressionVisitor(RenderContext context) {
		this.context = context;
		this.target = null;
	}

	NestedExpressionVisitor(RenderContext context, RenderTarget target) {
		this.context = context;
		this.target = target;
	}

	@Override
	Delegation enterMatched(NestedExpression segment) {
		builder.setLength(0);
		return super.enterMatched(segment);
	}

	@Override
	Delegation enterNested(Visitable segment) {

		DelegatingVisitor visitor = getDelegation(segment);

		return visitor != null ? Delegation.delegateTo(visitor) : Delegation.retain();
	}

	@Nullable
	private DelegatingVisitor getDelegation(Visitable segment) {

		if (segment instanceof Condition) {
			return conditionVisitor = new ConditionVisitor(context);
		}

		if (segment instanceof Expression) {
			return expressionVisitor = new ExpressionVisitor(context, ExpressionVisitor.AliasHandling.IGNORE);
		}

		return null;
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		if (conditionVisitor != null) {

			String part = "(" + conditionVisitor.getRenderedPart() + ")";
			if (target != null) {
				target.onRendered(part);
			} else {
				builder.append(part);
			}
			conditionVisitor = null;
		}

		if (expressionVisitor != null) {

			String part = "(" + expressionVisitor.getRenderedPart() + ")";
			if (target != null) {
				target.onRendered(part);
			} else {
				builder.append(part);
			}
			conditionVisitor = null;
		}

		return super.leaveNested(segment);
	}

	@Override
	public CharSequence getRenderedPart() {

		String part = builder.toString();
		builder.setLength(0);

		return part;
	}
}
