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
package org.springframework.data.relational.core.sql;

import org.springframework.util.Assert;

/**
 * Represents a {@code CAST} expression like {@code CAST(something AS JSON}.
 *
 * @author Jens Schauder
 * @since 2.3
 */
public class Cast extends AbstractSegment implements Expression {

	private final String targetType;
	private final Expression expression;

	private Cast(Expression expression, String targetType) {

		super(expression);

		Assert.notNull(targetType, "Cast target must not be null");

		this.expression = expression;
		this.targetType = targetType;
	}

	/**
	 * Creates a new {@code CAST} expression.
	 *
	 * @param expression the expression to cast. Must not be {@literal null}.
	 * @param targetType the type to cast to. Must not be {@literal null}.
	 * @return the {@code CAST} for {@code expression} into {@code targetType}.
	 */
	public static Expression create(Expression expression, String targetType) {
		return new Cast(expression, targetType);
	}

	public String getTargetType() {
		return targetType;
	}

	@Override
	public String toString() {
		return "CAST(" + expression + " AS " + targetType + ")";
	}
}
