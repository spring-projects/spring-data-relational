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
import org.springframework.util.Assert;

/**
 * {@link PartRenderer} for {@link Expression}s.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Sven Rienstra
 * @see Column
 * @see SubselectExpression
 */
class ExpressionVisitor extends TypedSubtreeVisitor<Expression> implements PartRenderer {

	private final RenderContext context;
	private final AliasHandling aliasHandling;

	private CharSequence value = "";
	private @Nullable PartRenderer partRenderer;

	/**
	 * Creates an {@code ExpressionVisitor} that does not use aliases for column names
	 *
	 * @param context must not be {@literal null}.
	 */
	ExpressionVisitor(RenderContext context) {
		this(context, AliasHandling.IGNORE);
	}

	/**
	 * Creates an {@code ExpressionVisitor}.
	 *
	 * @param context       must not be {@literal null}.
	 * @param aliasHandling controls if columns should be rendered as their alias or using their table names.
	 * @since 2.3
	 */
	ExpressionVisitor(RenderContext context, AliasHandling aliasHandling) {

		Assert.notNull(context, "The render context must not be null");
		Assert.notNull(aliasHandling, "The aliasHandling must not be null");

		this.context = context;
		this.aliasHandling = aliasHandling;
	}

	@Override
	Delegation enterMatched(Expression segment) {

		if (segment instanceof SubselectExpression) {

			SelectStatementVisitor visitor = new SelectStatementVisitor(context);
			partRenderer = visitor;
			return Delegation.delegateTo(visitor);
		}

		if (segment instanceof SimpleFunction) {

			SimpleFunctionVisitor visitor = new SimpleFunctionVisitor(context);
			partRenderer = visitor;
			return Delegation.delegateTo(visitor);
		}

		if (segment instanceof AnalyticFunction) {

			AnalyticFunctionVisitor visitor = new AnalyticFunctionVisitor(context);
			partRenderer = visitor;
			return Delegation.delegateTo(visitor);
		}

		if (segment instanceof Column column) {

			value = aliasHandling == AliasHandling.USE ? NameRenderer.fullyQualifiedReference(context, column)
					: NameRenderer.fullyQualifiedUnaliasedReference(context, column);
		} else if (segment instanceof BindMarker) {

			if (segment instanceof Named) {
				value = NameRenderer.render(context, (Named) segment);
			} else {
				value = segment.toString();
			}
		} else if (segment instanceof AsteriskFromTable asteriskFromTable) {

			TableLike table = asteriskFromTable.getTable();
			CharSequence renderedTable = table instanceof Aliased aliasedTable ? NameRenderer.render(context, aliasedTable)
					: NameRenderer.render(context, table);

			value = renderedTable + ".*";
		} else if (segment instanceof Cast) {

			CastVisitor visitor = new CastVisitor(context);
			partRenderer = visitor;
			return Delegation.delegateTo(visitor);
		} else if (segment instanceof CaseExpression) {

			CaseExpressionVisitor visitor = new CaseExpressionVisitor(context);
			partRenderer = visitor;
			return Delegation.delegateTo(visitor);
		} else {
			// works for literals and just and possibly more
			value = segment.toString();
		}

		return Delegation.retain();
	}

	@Override
	Delegation enterNested(Visitable segment) {

		if (segment instanceof Condition) {

			ConditionVisitor visitor = new ConditionVisitor(context);
			partRenderer = visitor;
			return Delegation.delegateTo(visitor);
		}

		if (segment instanceof InlineQuery) {

			NoopVisitor<InlineQuery> partRenderer = new NoopVisitor<>(InlineQuery.class);
			return Delegation.delegateTo(partRenderer);
		}
		return super.enterNested(segment);
	}

	@Override
	Delegation leaveMatched(Expression segment) {

		if (partRenderer != null) {

			value = partRenderer.getRenderedPart();
			partRenderer = null;
		}

		return super.leaveMatched(segment);
	}

	@Override
	public CharSequence getRenderedPart() {
		return value;
	}

	/**
	 * Describes how aliases of columns should be rendered.
	 *
	 * @since 2.3
	 */
	enum AliasHandling {
		/**
		 * The alias does not get used.
		 */
		IGNORE,

		/**
		 * The alias gets used. This means aliased columns get rendered as {@literal <alias>}.
		 */
		USE
	}
}
