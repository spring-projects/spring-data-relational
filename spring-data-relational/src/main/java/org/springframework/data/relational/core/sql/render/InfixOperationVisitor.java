/*
 * Copyright 2026-present the original author or authors.
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

import org.jspecify.annotations.Nullable;
import org.springframework.data.relational.core.sql.InfixOperation;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.util.Assert;

/**
 * Renders a {@link InfixOperation} by delegating to an {@link ExpressionVisitor} for each operand and joining the
 * rendered parts with the operator.
 *
 * @author Jens Schauder
 * @since 4.1.1
 */
class InfixOperationVisitor extends TypedSubtreeVisitor<InfixOperation> implements PartRenderer {

	private final RenderContext context;
	private @Nullable StringJoiner joiner;
	private @Nullable ExpressionVisitor expressionVisitor;

	InfixOperationVisitor(RenderContext context) {
		this.context = context;
	}

	@Override
	Delegation enterMatched(InfixOperation operation) {

		joiner = new StringJoiner(" " + operation.getOperator() + " ");
		return super.enterMatched(operation);
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
