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

import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.core.sql.From;
import org.springframework.data.relational.core.sql.Table;

/**
 * Renderer for {@link Table} used within a {@link From} clause. Uses a {@link RenderTarget} to call back for render
 * results.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
class FromTableVisitor extends TypedSubtreeVisitor<Table> {

	private final RenderContext context;
	private final RenderTarget parent;

	FromTableVisitor(RenderContext context, RenderTarget parent) {
		super();
		this.context = context;
		this.parent = parent;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#enterMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation enterMatched(Table segment) {

		StringBuilder builder = new StringBuilder();

		builder.append(context.getNamingStrategy().getName(segment));
		if (segment instanceof Aliased) {
			builder.append(" AS ").append(((Aliased) segment).getAlias());
		}

		parent.onRendered(builder);

		return super.enterMatched(segment);
	}
}
