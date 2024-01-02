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

import org.springframework.data.relational.core.sql.*;
import org.springframework.lang.Nullable;

/**
 * {@link org.springframework.data.relational.core.sql.Visitor} delegating {@link Condition} rendering to condition
 * {@link org.springframework.data.relational.core.sql.Visitor}s.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Daniele Canteri
 * @since 1.1
 * @see AndCondition
 * @see OrCondition
 * @see IsNull
 * @see Comparison
 * @see Like
 * @see In
 */
class ConditionVisitor extends TypedSubtreeVisitor<Condition> implements PartRenderer {

	private final RenderContext context;
	private final StringBuilder builder = new StringBuilder();

	ConditionVisitor(RenderContext context) {
		this.context = context;
	}

	@Override
	Delegation enterMatched(Condition segment) {

		DelegatingVisitor visitor = getDelegation(segment);

		return visitor != null ? Delegation.delegateTo(visitor) : Delegation.retain();
	}

	@Nullable
	private DelegatingVisitor getDelegation(Condition segment) {

		if (segment instanceof AndCondition) {
			return new MultiConcatConditionVisitor(context, (AndCondition) segment, builder::append);
		}

		if (segment instanceof OrCondition) {
			return new MultiConcatConditionVisitor(context, (OrCondition) segment, builder::append);
		}

		if (segment instanceof IsNull) {
			return new IsNullVisitor(context, builder::append);
		}

		if (segment instanceof Between) {
			return new BetweenVisitor((Between) segment, context, builder::append);
		}

		if (segment instanceof Comparison) {
			return new ComparisonVisitor(context, (Comparison) segment, builder::append);
		}

		if (segment instanceof Like) {
			return new LikeVisitor((Like) segment, context, builder::append);
		}

		if (segment instanceof In) {

			if (((In) segment).hasExpressions()) {
				return new InVisitor(context, builder::append);
			} else {
				return new EmptyInVisitor(context, builder::append);
			}
		}

		if (segment instanceof NestedCondition) {
			return new NestedConditionVisitor(context, builder::append);
		}

		if (segment instanceof ConstantCondition) {
			return new ConstantConditionVisitor(context, builder::append);
		}

		if (segment instanceof Not) {
			return new NotConditionVisitor(context, builder::append);
		}

		return null;
	}

	@Override
	public CharSequence getRenderedPart() {
		return builder;
	}
}
