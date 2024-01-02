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

import org.springframework.data.relational.core.sql.Aliased;
import org.springframework.data.relational.core.sql.From;
import org.springframework.data.relational.core.sql.InlineQuery;
import org.springframework.data.relational.core.sql.TableLike;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Renderer for {@link TableLike} used within a {@link From} or
 * {@link org.springframework.data.relational.core.sql.Join} clause. Uses a {@link RenderTarget} to call back for render
 * results.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
class FromTableVisitor extends TypedSubtreeVisitor<TableLike> {

	private final RenderContext context;
	private final RenderTarget parent;
	@Nullable private StringBuilder builder = null;

	FromTableVisitor(RenderContext context, RenderTarget parent) {
		super();
		this.context = context;
		this.parent = parent;
	}

	@Override
	Delegation enterMatched(TableLike segment) {

		builder = new StringBuilder();

		if (segment instanceof InlineQuery) {
			return Delegation.delegateTo(new SubselectVisitor(context, builder::append));
		}

		return super.enterMatched(segment);
	}

	@Override
	Delegation leaveMatched(TableLike segment) {

		Assert.state(builder != null, "Builder must not be null in leaveMatched");

		builder.append(NameRenderer.render(context, segment));
		if (segment instanceof Aliased) {
			builder.append(" ").append(NameRenderer.render(context, (Aliased) segment));
		}

		parent.onRendered(builder);

		return super.leaveMatched(segment);
	}
}
