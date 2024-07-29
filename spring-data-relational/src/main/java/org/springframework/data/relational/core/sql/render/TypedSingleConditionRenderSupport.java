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
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.data.relational.core.sql.When;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Support class for {@link TypedSubtreeVisitor typed visitors} that want to render a single {@link Condition} and
 * delegate nested {@link Expression} and {@link Condition} rendering.
 *
 * @author Mark Paluch
 * @author Sven Rienstra
 * @since 1.1
 */
abstract class TypedSingleConditionRenderSupport<T extends Visitable> extends TypedSubtreeVisitor<T> {

	private final RenderContext context;
	private @Nullable PartRenderer current;

	TypedSingleConditionRenderSupport(RenderContext context) {
		this.context = context;
	}

	@Override
	Delegation enterNested(Visitable segment) {

		if (segment instanceof When) {
			WhenVisitor visitor = new WhenVisitor(context);
			current = visitor;
			return Delegation.delegateTo(visitor);
		}

		if (segment instanceof Condition) {
			ConditionVisitor visitor = new ConditionVisitor(context);
			current = visitor;
			return Delegation.delegateTo(visitor);
		}

		if (segment instanceof Expression) {
			ExpressionVisitor visitor = new ExpressionVisitor(context);
			current = visitor;
			return Delegation.delegateTo(visitor);
		}

		throw new IllegalStateException("Cannot provide visitor for " + segment);
	}

	/**
	 * Returns whether rendering was delegated to a {@link ExpressionVisitor} or {@link ConditionVisitor}.
	 *
	 * @return {@literal true} when rendering was delegated to a {@link ExpressionVisitor} or {@link ConditionVisitor}.
	 */
	protected boolean hasDelegatedRendering() {
		return current != null;
	}

	/**
	 * Consumes the delegated rendering part. Call {@link #hasDelegatedRendering()} to check whether rendering was
	 * actually delegated. Consumption releases the delegated rendered.
	 *
	 * @return the delegated rendered part.
	 * @throws IllegalStateException if rendering was not delegate.
	 */
	protected CharSequence consumeRenderedPart() {

		Assert.state(hasDelegatedRendering(), "Rendering not delegated; Cannot consume delegated rendering part");

		PartRenderer current = this.current;
		this.current = null;
		return current.getRenderedPart();
	}
}
