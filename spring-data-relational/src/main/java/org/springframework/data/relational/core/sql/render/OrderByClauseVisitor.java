/*
 * Copyright 2019-2021 the original author or authors.
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

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.OrderByField;
import org.springframework.data.relational.core.sql.Visitable;

/**
 * {@link PartRenderer} for {@link OrderByField}s.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
class OrderByClauseVisitor extends TypedSubtreeVisitor<OrderByField> implements PartRenderer {

	private final RenderContext context;

	private final StringBuilder builder = new StringBuilder();
	private boolean first = true;

	OrderByClauseVisitor(RenderContext context) {
		this.context = context;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#enterMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation enterMatched(OrderByField segment) {

		if (!first) {
			builder.append(", ");
		}
		first = false;

		return super.enterMatched(segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#leaveMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveMatched(OrderByField segment) {

		OrderByField field = segment;

		if (field.getDirection() != null) {
			builder.append(" ") //
					.append(field.getDirection());
		}

		return Delegation.leave();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#leaveNested(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveNested(Visitable segment) {

		if (segment instanceof Column) {
			builder.append(NameRenderer.fullyQualifiedReference(context, (Column) segment));
		}

		return super.leaveNested(segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.PartRenderer#getRenderedPart()
	 */
	@Override
	public CharSequence getRenderedPart() {
		return builder;
	}
}
