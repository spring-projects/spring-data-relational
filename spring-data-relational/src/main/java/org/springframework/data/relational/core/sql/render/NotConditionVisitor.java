/*
 * Copyright 2019-2023 the original author or authors.
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
import org.springframework.data.relational.core.sql.NestedCondition;
import org.springframework.data.relational.core.sql.Not;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.lang.Nullable;

/**
 * Renderer for {@link Not}. Uses a {@link RenderTarget} to call back for render results.
 *
 * @author Jens Schauder
 * @since 3.2
 */
class NotConditionVisitor extends TypedSubtreeVisitor<NestedCondition> {

	private final RenderContext context;
	private final RenderTarget target;

	private @Nullable ConditionVisitor conditionVisitor;

	NotConditionVisitor(RenderContext context, RenderTarget target) {

		this.context = context;
		this.target = target;
	}

	@Override
	Delegation enterNested(Visitable segment) {

		DelegatingVisitor visitor = getDelegation(segment);

		return visitor != null ? Delegation.delegateTo(visitor) : Delegation.retain();
	}

	@Nullable
	private DelegatingVisitor getDelegation(Visitable segment) {

		if (segment instanceof Condition) {
			return conditionVisitor = new ConditionVisitor(context);
		}

		return null;
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		if (conditionVisitor != null) {

			target.onRendered("NOT (" + conditionVisitor.getRenderedPart() + ")");
			conditionVisitor = null;
		}

		return super.leaveNested(segment);
	}
}
