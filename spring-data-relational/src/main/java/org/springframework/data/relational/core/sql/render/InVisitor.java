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

import org.springframework.data.relational.core.sql.In;
import org.springframework.data.relational.core.sql.Visitable;

/**
 * Renderer for {@link In}. Uses a {@link RenderTarget} to call back for render results.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
class InVisitor extends TypedSingleConditionRenderSupport<In> {

	private final RenderTarget target;
	private final StringBuilder part = new StringBuilder();
	private boolean needsComma = false;
	private boolean notIn = false;

	InVisitor(RenderContext context, RenderTarget target) {
		super(context);
		this.target = target;
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		if (hasDelegatedRendering()) {
			CharSequence renderedPart = consumeRenderedPart();

			if (needsComma) {
				part.append(", ");
			}

			if (part.isEmpty()) {
				part.append(renderedPart);
				if (notIn) {
					part.append(" NOT");
				}
				part.append(" IN (");
			} else {
				part.append(renderedPart);
				needsComma = true;
			}
		}

		return super.leaveNested(segment);
	}

	@Override
	Delegation enterMatched(In segment) {

		notIn = segment.isNotIn();

		return super.enterMatched(segment);
	}

	@Override
	Delegation leaveMatched(In segment) {

		part.append(")");
		target.onRendered(part);

		return super.leaveMatched(segment);
	}
}
