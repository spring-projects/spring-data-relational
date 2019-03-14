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

import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.core.sql.AsteriskFromTable;
import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SelectList;
import org.springframework.data.relational.core.sql.SimpleFunction;
import org.springframework.data.relational.core.sql.Table;
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
	// subelements.


	SelectListVisitor(RenderContext context, RenderTarget target) {
		this.context = context;
		this.target = target;
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
		if (segment instanceof SimpleFunction) {
			builder.append(((SimpleFunction) segment).getFunctionName()).append("(");
			insideFunction = true;
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

		if (segment instanceof Table) {
			builder.append(context.getNamingStrategy().getReferenceName((Table) segment)).append('.');
		}

		if (segment instanceof SimpleFunction) {

			builder.append(")");
			if (segment instanceof Aliased) {
				builder.append(" AS ").append(((Aliased) segment).getAlias());
			}

			insideFunction = false;
			requiresComma = true;
		} else if (segment instanceof Column) {

			builder.append(context.getNamingStrategy().getName((Column) segment));
			if (segment instanceof Aliased && !insideFunction) {
				builder.append(" AS ").append(((Aliased) segment).getAlias());
			}
			requiresComma = true;
		} else if (segment instanceof AsteriskFromTable) {
			// the toString of AsteriskFromTable includes the table name, which would cause it to appear twice.
			builder.append("*");
		} else if (segment instanceof Expression) {
			builder.append(segment.toString());
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
