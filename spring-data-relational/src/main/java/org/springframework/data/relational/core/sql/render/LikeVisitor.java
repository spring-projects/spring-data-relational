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

import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Like;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.lang.Nullable;

/**
 * {@link org.springframework.data.relational.core.sql.Visitor} rendering comparison {@link Condition}. Uses a
 * {@link RenderTarget} to call back for render results.
 *
 * @author Mark Paluch
 * @see Like
 * @since 1.1
 */
class LikeVisitor extends FilteredSubtreeVisitor {

	private final Like like;
	private final RenderContext context;
	private final RenderTarget target;
	private final StringBuilder part = new StringBuilder();
	private @Nullable PartRenderer current;

	LikeVisitor(Like condition, RenderContext context, RenderTarget target) {
		super(it -> it == condition);
		this.like = condition;
		this.context = context;
		this.target = target;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.FilteredSubtreeVisitor#enterNested(org.springframework.data.relational.core.sql.Visitable)
	 */
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.FilteredSubtreeVisitor#leaveNested(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveNested(Visitable segment) {

		if (current != null) {
			if (part.length() != 0) {

				part.append(' ');

				if (like.isNegated()) {
					part.append("NOT ");
				}

				part.append("LIKE ");
			}

			part.append(current.getRenderedPart());
			current = null;
		}

		return super.leaveNested(segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.FilteredSubtreeVisitor#leaveMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveMatched(Visitable segment) {

		target.onRendered(part);

		return super.leaveMatched(segment);
	}
}
