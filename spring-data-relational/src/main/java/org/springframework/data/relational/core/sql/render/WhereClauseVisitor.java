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

import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.data.relational.core.sql.Where;

/**
 * Renderer for {@link Where} segments. Uses a {@link RenderTarget} to call back for render results.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
class WhereClauseVisitor extends TypedSubtreeVisitor<Where> {

	private final RenderTarget parent;
	private final ConditionVisitor conditionVisitor;

	WhereClauseVisitor(RenderContext context, RenderTarget parent) {
		this.conditionVisitor = new ConditionVisitor(context);
		this.parent = parent;
	}

	@Override
	Delegation enterNested(Visitable segment) {

		if (segment instanceof Condition) {
			return Delegation.delegateTo(conditionVisitor);
		}

		return super.enterNested(segment);
	}

	@Override
	Delegation leaveMatched(Where segment) {

		parent.onRendered(conditionVisitor.getRenderedPart());
		return super.leaveMatched(segment);
	}
}
