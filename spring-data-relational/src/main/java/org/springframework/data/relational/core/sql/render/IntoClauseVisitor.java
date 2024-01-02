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

import org.springframework.data.relational.core.sql.Into;
import org.springframework.data.relational.core.sql.Visitable;

/**
 * Renderer for {@link Into}. Uses a {@link RenderTarget} to call back for render results.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class IntoClauseVisitor extends TypedSubtreeVisitor<Into> {

	private final FromTableVisitor visitor;
	private final RenderTarget parent;
	private final StringBuilder builder = new StringBuilder();
	private boolean first = true;

	IntoClauseVisitor(RenderContext context, RenderTarget parent) {

		this.visitor = new FromTableVisitor(context, it -> {

			if (first) {
				first = false;
			} else {
				builder.append(", ");
			}

			builder.append(it);
		});

		this.parent = parent;
	}

	@Override
	Delegation enterNested(Visitable segment) {
		return Delegation.delegateTo(visitor);
	}

	@Override
	Delegation leaveMatched(Into segment) {
		parent.onRendered(builder);
		return super.leaveMatched(segment);
	}
}
