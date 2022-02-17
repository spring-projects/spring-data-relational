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

import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SelectList;
import org.springframework.data.relational.core.sql.Visitable;

/**
 * {@link PartRenderer} for {@link SelectList}s.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
class SelectListVisitor extends TypedSubtreeVisitor<SelectList> implements PartRenderer {

	private final RenderContext context;
	private final StringBuilder builder = new StringBuilder();
	private final RenderTarget target;
	private boolean requiresComma = false;
	private boolean insideFunction = false; // this is hackery and should be fix with a proper visitor for
	private ExpressionVisitor expressionVisitor;
	// subelements.

	SelectListVisitor(RenderContext context, RenderTarget target) {

		this.context = context;
		this.target = target;
		this.expressionVisitor = new ExpressionVisitor(context, ExpressionVisitor.AliasHandling.IGNORE);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#enterNested(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation enterNested(Visitable segment) {

		if (requiresComma) {
			builder.append(", ");
			requiresComma = false;
		}
		if (segment instanceof Expression) {
			return Delegation.delegateTo(expressionVisitor);
		}

		return super.enterNested(segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#leaveMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveMatched(SelectList segment) {

		target.onRendered(builder);
		return super.leaveMatched(segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#leaveNested(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveNested(Visitable segment) {

		if (segment instanceof Expression) {

			builder.append(expressionVisitor.getRenderedPart());
			requiresComma = true;
		}

		if (segment instanceof Aliased && !insideFunction) {
			builder.append(" AS ").append(NameRenderer.render(context, (Aliased) segment));
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
