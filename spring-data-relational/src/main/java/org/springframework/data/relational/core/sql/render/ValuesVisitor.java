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

import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Values;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.lang.Nullable;

/**
 * Renderer for {@link Values}. Uses a {@link RenderTarget} to call back for render results.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class ValuesVisitor extends TypedSubtreeVisitor<Values> {

	private final RenderTarget parent;
	private final StringBuilder builder = new StringBuilder();
	private final RenderContext context;

	private @Nullable ExpressionVisitor current;
	private boolean first = true;

	ValuesVisitor(RenderContext context, RenderTarget parent) {

		this.context = context;
		this.parent = parent;
	}

	@Override
	Delegation enterNested(Visitable segment) {

		if (segment instanceof Expression) {
			this.current = new ExpressionVisitor(context);
			return Delegation.delegateTo(this.current);
		}

		return super.enterNested(segment);
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		if (this.current != null) {

			if (first) {
				first = false;
			} else {
				builder.append(", ");
			}

			builder.append(this.current.getRenderedPart());
			this.current = null;
		}

		return super.leaveNested(segment);
	}

	@Override
	Delegation leaveMatched(Values segment) {
		parent.onRendered(builder);
		return super.leaveMatched(segment);
	}
}
