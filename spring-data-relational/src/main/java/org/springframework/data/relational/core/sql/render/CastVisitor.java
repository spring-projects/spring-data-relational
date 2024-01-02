/*
 * Copyright 2021-2024 the original author or authors.
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

import java.util.StringJoiner;

import org.springframework.data.relational.core.sql.Cast;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Renders a CAST expression, by delegating to an {@link ExpressionVisitor} and building the expression out of the
 * rendered parts.
 * 
 * @author Jens Schauder
 * @since 2.3
 */
class CastVisitor extends TypedSubtreeVisitor<Cast> implements PartRenderer {

	private final RenderContext context;
	@Nullable private StringJoiner joiner;
	@Nullable private ExpressionVisitor expressionVisitor;

	CastVisitor(RenderContext context) {

		this.context = context;
	}

	@Override
	Delegation enterMatched(Cast cast) {

		joiner = new StringJoiner(", ", "CAST(", " AS " + cast.getTargetType() + ")");

		return super.enterMatched(cast);
	}

	@Override
	Delegation enterNested(Visitable segment) {

		expressionVisitor = new ExpressionVisitor(context, ExpressionVisitor.AliasHandling.IGNORE);
		return Delegation.delegateTo(expressionVisitor);
	}

	@Override
	Delegation leaveNested(Visitable segment) {

		Assert.state(joiner != null, "Joiner must not be null");
		Assert.state(expressionVisitor != null, "ExpressionVisitor must not be null");

		joiner.add(expressionVisitor.getRenderedPart());
		return super.leaveNested(segment);
	}

	@Override
	public CharSequence getRenderedPart() {

		if (joiner == null) {
			throw new IllegalStateException("Joiner must not be null");
		}

		return joiner.toString();
	}
}
