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

import org.springframework.data.relational.core.sql.IsNull;
import org.springframework.data.relational.core.sql.Visitable;

/**
 * Renderer for {@link IsNull}. Uses a {@link RenderTarget} to call back for render results.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
class IsNullVisitor extends TypedSingleConditionRenderSupport<IsNull> {

	private final RenderTarget target;
	private final StringBuilder part = new StringBuilder();

	IsNullVisitor(RenderContext context, RenderTarget target) {
		super(context);
		this.target = target;
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		if (hasDelegatedRendering()) {
			part.append(consumeRenderedPart());
		}

		return super.leaveNested(segment);
	}

	@Override
	Delegation leaveMatched(IsNull segment) {

		if (segment.isNegated()) {
			part.append(" IS NOT NULL");
		} else {
			part.append(" IS NULL");
		}

		target.onRendered(part);

		return super.leaveMatched(segment);
	}
}
