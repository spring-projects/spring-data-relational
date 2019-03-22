/*
 * Copyright 2019 the original author or authors.
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

import org.springframework.data.relational.core.sql.BindMarker;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Literal;
import org.springframework.data.relational.core.sql.Named;
import org.springframework.data.relational.core.sql.SubselectExpression;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.lang.Nullable;

/**
 * {@link PartRenderer} for {@link Expression}s.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 * @see Column
 * @see SubselectExpression
 */
class ExpressionVisitor extends TypedSubtreeVisitor<Expression> implements PartRenderer {

	private final RenderContext context;

	private CharSequence value = "";
	private @Nullable PartRenderer partRenderer;

	ExpressionVisitor(RenderContext context) {
		this.context = context;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#enterMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation enterMatched(Expression segment) {

		if (segment instanceof SubselectExpression) {

			SelectStatementVisitor visitor = new SelectStatementVisitor(context);
			partRenderer = visitor;
			return Delegation.delegateTo(visitor);
		}

		if (segment instanceof Column) {

			RenderNamingStrategy namingStrategy = context.getNamingStrategy();
			Column column = (Column) segment;

			value = namingStrategy.getReferenceName(column.getTable()) + "." + namingStrategy.getReferenceName(column);
		} else if (segment instanceof BindMarker) {

			if (segment instanceof Named) {
				value = ((Named) segment).getName();
			} else {
				value = segment.toString();
			}
		} else if (segment instanceof Literal) {
			value = segment.toString();
		}

		return Delegation.retain();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#enterNested(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation enterNested(Visitable segment) {

		if (segment instanceof Condition) {
			ConditionVisitor visitor = new ConditionVisitor(context);
			partRenderer = visitor;
			return Delegation.delegateTo(visitor);
		}

		return super.enterNested(segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#leaveMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveMatched(Expression segment) {

		if (partRenderer != null) {
			value = partRenderer.getRenderedPart();
			partRenderer = null;
		}

		return super.leaveMatched(segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.PartRenderer#getRenderedPart()
	 */
	@Override
	public CharSequence getRenderedPart() {
		return value;
	}
}
