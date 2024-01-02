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
package org.springframework.data.relational.core.sql;

import org.springframework.util.Assert;

/**
 * {@code IS NULL} {@link Condition}.
 *
 * @author Jens Schauder
 * @since 1.1
 */
public class IsNull extends AbstractSegment implements Condition {

	private final Expression expression;
	private final boolean negated;

	private IsNull(Expression expression) {
		this(expression, false);
	}

	private IsNull(Expression expression, boolean negated) {

		super(expression);

		this.expression = expression;
		this.negated = negated;
	}

	/**
	 * Creates a new {@link IsNull} expression.
	 *
	 * @param expression must not be {@literal null}.
	 * @return
	 */
	public static IsNull create(Expression expression) {

		Assert.notNull(expression, "Expression must not be null");

		return new IsNull(expression);
	}

	@Override
	public Condition not() {
		return new IsNull(expression, !negated);
	}

	public boolean isNegated() {
		return negated;
	}

	@Override
	public String toString() {
		return expression + (negated ? " IS NOT NULL" : " IS NULL");
	}
}
