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
package org.springframework.data.relational.core.sql;

import org.springframework.util.Assert;

/**
 * Represents an infix (two expressions combined with an operator) operation between two {@link Expression}s such as
 * arithmetic operations {@code a + b} or concatenation {@code a || b}.
 * <p>
 * Results in a rendered expression: {@code <left> <operator> <right>} (e.g. {@code col + 4}). For conditional
 * expressions use {@link Comparison} instead of a raw {@code InfixOperation}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 4.1.1
 * @see Comparison
 */
public class InfixOperation extends AbstractSegment implements Expression {

	private final Expression left;
	private final String operator;
	private final Expression right;

	private InfixOperation(Expression left, String operator, Expression right) {
		super(left, right);
		this.left = left;
		this.operator = operator;
		this.right = right;
	}

	/**
	 * Create a new {@link InfixOperation} using {@link Expression}s and an operator.
	 *
	 * @param left the left {@link Expression}.
	 * @param operator the operator.
	 * @param right the right {@link Expression}.
	 * @return the {@link InfixOperation} operation.
	 */
	public static InfixOperation create(Expression left, String operator, Expression right) {

		Assert.notNull(left, "Left expression must not be null");
		Assert.notNull(operator, "Operator must not be null");
		Assert.notNull(right, "Right expression must not be null");

		return new InfixOperation(left, operator, right);
	}

	public Expression getLeft() {
		return left;
	}

	public String getOperator() {
		return operator;
	}

	public Expression getRight() {
		return right;
	}

	/**
	 * Creates a new {@code InfixOperation} aliased to {@code alias}.
	 *
	 * @param alias must not be {@literal null} or empty.
	 * @return the new {@code InfixOperation} using the {@code alias}.
	 * @since 4.1.1
	 */
	public InfixOperation as(String alias) {
		return new AliasedInfixOperation(this, SqlIdentifier.unquoted(alias));
	}

	/**
	 * Creates a new {@code InfixOperation} aliased to {@code alias}.
	 *
	 * @param alias must not be {@literal null} or empty.
	 * @return the new {@code InfixOperation} using the {@code alias}.
	 * @since 4.1.1
	 */
	public InfixOperation as(SqlIdentifier alias) {
		return new AliasedInfixOperation(this, alias);
	}

	@Override
	public String toString() {
		return left + " " + operator + " " + right;
	}

	private static class AliasedInfixOperation extends InfixOperation implements Aliased {

		private final SqlIdentifier alias;

		AliasedInfixOperation(InfixOperation operation, SqlIdentifier alias) {

			super(operation.left, operation.operator, operation.right);
			this.alias = alias;
		}

		@Override
		public SqlIdentifier getAlias() {
			return alias;
		}

	}

}
