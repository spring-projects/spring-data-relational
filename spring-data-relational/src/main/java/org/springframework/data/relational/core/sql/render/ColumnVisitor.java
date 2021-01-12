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
import org.springframework.data.relational.core.sql.Table;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.lang.Nullable;

/**
 * Renderer for {@link Column}s.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class ColumnVisitor extends TypedSubtreeVisitor<Column> {

	private final RenderContext context;
	private final RenderTarget target;
	private final boolean considerTablePrefix;

	private @Nullable String tableName;

	ColumnVisitor(RenderContext context, boolean considerTablePrefix, RenderTarget target) {
		this.context = context;
		this.target = target;
		this.considerTablePrefix = considerTablePrefix;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#leaveMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveMatched(Column segment) {

		String column = context.getNamingStrategy().getName(segment);
		StringBuilder builder = new StringBuilder(
				tableName != null ? tableName.length() + column.length() : column.length());
		if (considerTablePrefix && tableName != null) {
			builder.append(tableName);
		}
		builder.append(column);

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
			tableName = context.getNamingStrategy().getReferenceName((Table) segment) + '.';
		}

		return super.leaveNested(segment);
	}
}
