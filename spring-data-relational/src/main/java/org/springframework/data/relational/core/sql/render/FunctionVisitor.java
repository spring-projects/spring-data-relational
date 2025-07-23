/*
 * Copyright 2019-2025 the original author or authors.
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

import org.springframework.data.relational.core.sql.BaseFunction;
import org.springframework.data.relational.core.sql.Visitable;

/**
 * Renderer for {@link org.springframework.data.relational.core.sql.SimpleFunction}. Uses a {@link RenderTarget} to call
 * back for render results.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
class FunctionVisitor extends TypedSingleConditionRenderSupport<BaseFunction> implements PartRenderer {

	private final StringBuilder part = new StringBuilder();
	private boolean needsComma = false;

	FunctionVisitor(RenderContext context) {
		super(context);
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		if (hasDelegatedRendering()) {

			if (needsComma) {
				part.append(", ");
			}

			part.append(consumeRenderedPart());
			needsComma = true;
		}

		return super.leaveNested(segment);
	}

	@Override
	Delegation enterMatched(BaseFunction segment) {

		part.append(segment.getFunctionName()).append(segment.getBeforeArgs());

		return super.enterMatched(segment);
	}

	@Override
	Delegation leaveMatched(BaseFunction segment) {

		part.append(segment.getAfterArgs());

		return super.leaveMatched(segment);
	}

	@Override
	public CharSequence getRenderedPart() {
		return part;
	}
}
