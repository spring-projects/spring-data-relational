/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.data.relational.core.sqlgeneration;

import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;

import java.util.Objects;

/**
 * Pattern matching a substraction expression
 *
 * @author Jens Schauder
 */
class SubtractionPattern extends TypedExpressionPattern<Subtraction> {

	private final ExpressionPattern left;
	private final ExpressionPattern right;

	SubtractionPattern(ExpressionPattern left, ExpressionPattern right) {

		super(Subtraction.class);

		this.left = left;
		this.right = right;
	}

	@Override
	public boolean matches(Subtraction subtraction) {
		return left.matches(subtraction.getLeftExpression()) && right.matches(subtraction.getRightExpression());
	}

	@Override
	public String toString() {
		return left + " - " + right;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || getClass() != o.getClass()) return false;
		SubtractionPattern that = (SubtractionPattern) o;
		return Objects.equals(left, that.left) && Objects.equals(right, that.right);
	}

	@Override
	public int hashCode() {
		return Objects.hash(left, right);
	}
}
