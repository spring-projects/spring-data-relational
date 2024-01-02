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

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.relational.core.sql.TableLike;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.lang.Nullable;

/**
 * Renderer for {@link Column}s. Renders a column as {@literal &gt;table&lt;.&gt;column&lt;} or
 * {@literal &gt;column&lt;}.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class ColumnVisitor extends TypedSubtreeVisitor<Column> {

	private final RenderContext context;
	private final RenderTarget target;
	private final boolean considerTablePrefix;

	private @Nullable SqlIdentifier tableName;

	ColumnVisitor(RenderContext context, boolean considerTablePrefix, RenderTarget target) {
		this.context = context;
		this.target = target;
		this.considerTablePrefix = considerTablePrefix;
	}

	@Override
	Delegation leaveMatched(Column segment) {

		SqlIdentifier column = context.getNamingStrategy().getName(segment);

		CharSequence name = considerTablePrefix && tableName != null
				? NameRenderer.render(context, SqlIdentifier.from(tableName, column))
				: NameRenderer.render(context, segment);

		target.onRendered(name);
		return super.leaveMatched(segment);
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		if (segment instanceof TableLike) {
			tableName = context.getNamingStrategy().getReferenceName((TableLike) segment);
		}

		return super.leaveNested(segment);
	}
}
