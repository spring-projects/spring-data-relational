/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.relational.core.sql.render;

import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Equals;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Visitable;

/**
 * {@link org.springframework.data.relational.core.sql.Visitor} rendering comparison {@link Condition}. Uses a
 * {@link RenderTarget} to call back for render results.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @see Equals
 */
class ComparisonVisitor extends FilteredSubtreeVisitor {

	private final RenderTarget target;
	private final String comparator;
	private final StringBuilder part = new StringBuilder();
	private PartRenderer current;

	ComparisonVisitor(Equals condition, RenderTarget target) {
		super(it -> it == condition);
		this.target = target;
		this.comparator = " = ";
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.FilteredSubtreeVisitor#enterNested(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation enterNested(Visitable segment) {

		if (segment instanceof Expression) {
			ExpressionVisitor visitor = new ExpressionVisitor();
			current = visitor;
			return Delegation.delegateTo(visitor);
		}

		if (segment instanceof Condition) {
			ConditionVisitor visitor = new ConditionVisitor();
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
				part.append(comparator);
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
