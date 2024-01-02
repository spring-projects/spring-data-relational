/*
 * Copyright 2021-2024 the original author or authors.
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

import org.springframework.data.relational.core.sql.AnalyticFunction;
import org.springframework.data.relational.core.sql.OrderBy;
import org.springframework.data.relational.core.sql.SimpleFunction;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.lang.Nullable;

/**
 * Renderer for {@link AnalyticFunction}. Uses a {@link RenderTarget} to call back for render results.
 *
 * @author Jens Schauder
 * @since 2.7
 */
class AnalyticFunctionVisitor extends TypedSingleConditionRenderSupport<AnalyticFunction> implements PartRenderer {

	private final StringBuilder part = new StringBuilder();
	private final RenderContext context;
	@Nullable private PartRenderer delegate;
	private boolean addSpace = false;

	AnalyticFunctionVisitor(RenderContext context) {
		super(context);
		this.context = context;
	}

	@Override
	Delegation enterNested(Visitable segment) {

		if (segment instanceof SimpleFunction) {

			delegate = new SimpleFunctionVisitor(context);
			return Delegation.delegateTo((DelegatingVisitor) delegate);
		}

		if (segment instanceof AnalyticFunction.Partition) {

			delegate = new SegmentListVisitor("PARTITION BY ", ", ", new ExpressionVisitor(context));
			return Delegation.delegateTo((DelegatingVisitor) delegate);
		}

		if (segment instanceof OrderBy) {

			delegate = new SegmentListVisitor("ORDER BY ", ", ", new OrderByClauseVisitor(context));
			return Delegation.delegateTo((DelegatingVisitor) delegate);
		}
		return super.enterNested(segment);
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		if (delegate instanceof SimpleFunctionVisitor) {

			part.append(delegate.getRenderedPart());
			part.append(" OVER(");
		}

		if (delegate instanceof SegmentListVisitor) {

			final CharSequence renderedPart = delegate.getRenderedPart();
			if (renderedPart.length() != 0) {

				if (addSpace) {
					part.append(' ');
				}
				part.append(renderedPart);
				addSpace = true;
			}
		}

		return super.leaveNested(segment);
	}

	@Override
	Delegation leaveMatched(AnalyticFunction segment) {

		part.append(")");

		return super.leaveMatched(segment);
	}

	@Override
	public CharSequence getRenderedPart() {
		return part;
	}
}
