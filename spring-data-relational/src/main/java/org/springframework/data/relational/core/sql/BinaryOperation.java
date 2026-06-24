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
 * Represents a binary operation between two {@link Expression}s such as {@code a + b} or {@code a || b}.
 *
 * @author Jens Schauder
 * @since 4.2
 */
public class BinaryOperation extends AbstractSegment implements Expression {

	private final Expression left;
	private final String operator;
	private final Expression right;

	BinaryOperation(Expression left, String operator, Expression right) {

		super(left, right);

		Assert.notNull(left, "Left expression must not be null");
		Assert.notNull(operator, "Operator must not be null");
		Assert.notNull(right, "Right expression must not be null");

		this.left = left;
		this.operator = operator;
		this.right = right;
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

	public BinaryOperation as(String alias) {
		return new AliasedBinaryOperation(this, SqlIdentifier.unquoted(alias));
	}

	public BinaryOperation as(SqlIdentifier alias) {
		return new AliasedBinaryOperation(this, alias);
	}

	@Override
	public String toString() {
		return left + " " + operator + " " + right;
	}

	private static class AliasedBinaryOperation extends BinaryOperation implements Aliased {

		private final SqlIdentifier alias;

		AliasedBinaryOperation(BinaryOperation operation, SqlIdentifier alias) {

			super(operation.left, operation.operator, operation.right);
			this.alias = alias;
		}

		@Override
		public SqlIdentifier getAlias() {
			return alias;
		}
	}
}
