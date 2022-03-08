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

import org.springframework.data.relational.core.sql.AndCondition;
import org.springframework.data.relational.core.sql.OrCondition;
import org.springframework.data.relational.core.sql.Visitable;

/**
 * Renderer for {@link AndCondition} and {@link OrCondition}. Uses a {@link RenderTarget} to call back for render
 * results.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
class MultiConcatConditionVisitor extends FilteredSingleConditionRenderSupport {

	private final RenderTarget target;
	private final String concat;
	private final StringBuilder part = new StringBuilder();

	MultiConcatConditionVisitor(RenderContext context, AndCondition condition, RenderTarget target) {
		super(context, it -> it == condition);
		this.target = target;
		this.concat = " AND ";
	}

	MultiConcatConditionVisitor(RenderContext context, OrCondition condition, RenderTarget target) {
		super(context, it -> it == condition);
		this.target = target;
		this.concat = " OR ";
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.FilteredSubtreeVisitor#leaveNested(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveNested(Visitable segment) {

		if (hasDelegatedRendering()) {
			if (part.length() != 0) {
				part.append(concat);
			}

			part.append(consumeRenderedPart());
		}

		return super.leaveNested(segment);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.FilteredSubtreeVisitor#leaveMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
	@Override
	Delegation leaveMatched(Visitable segment) {

		target.onRendered(part);

		return super.leaveMatched(segment);
	}
}
