/*
 * Copyright 2019-2020 the original author or authors.
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
import org.springframework.data.relational.core.sql.Between;
import org.springframework.data.relational.core.sql.Comparison;
import org.springframework.data.relational.core.sql.Condition;
import org.springframework.data.relational.core.sql.In;
import org.springframework.data.relational.core.sql.IsNull;
import org.springframework.data.relational.core.sql.Like;
import org.springframework.data.relational.core.sql.NestedCondition;
import org.springframework.data.relational.core.sql.OrCondition;
import org.springframework.lang.Nullable;

/**
 * {@link org.springframework.data.relational.core.sql.Visitor} delegating {@link Condition} rendering to condition
 * {@link org.springframework.data.relational.core.sql.Visitor}s.
 *
 * @author Mark Paluch
 * @author Jens Schauder
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.TypedSubtreeVisitor#enterMatched(org.springframework.data.relational.core.sql.Visitable)
	 */
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
			return new InVisitor(context, builder::append);
		}

		if (segment instanceof NestedCondition) {
			return new NestedConditionVisitor(context, builder::append);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.render.PartRenderer#getRenderedPart()
	 */
	@Override
	public CharSequence getRenderedPart() {
		return builder;
	}
}
