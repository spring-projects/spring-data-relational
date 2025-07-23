/*
 * Copyright 2019-2025 the original author or authors.
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
 * Applying an operator to a source {@link Expression}.
 * <p>
 * Results in a rendered condition: {@code <left> <operator> <right>} (e.g. {@code col -> index}.
 * </p>
 *
 * @author Mark Paluch
 * @since 4.0
 */
public class OperatorExpression extends AbstractSegment implements Expression {

	private final Expression left;
	private final String operator;
	private final Expression right;

	protected OperatorExpression(Expression left, String operator, Expression right) {

		super(left, right);

		this.left = left;
		this.operator = operator;
		this.right = right;
	}

	/**
	 * Creates a new {@link OperatorExpression} {@link Condition} given two {@link Expression}s.
	 *
	 * @param leftColumnOrExpression the left {@link Expression}.
	 * @param operator the operator.
	 * @param rightColumnOrExpression the right {@link Expression}.
	 * @return the {@link OperatorExpression} condition.
	 */
	public static OperatorExpression create(Expression leftColumnOrExpression, String operator,
			Expression rightColumnOrExpression) {

		Assert.notNull(leftColumnOrExpression, "Left expression must not be null");
		Assert.notNull(operator, "Comparator must not be null");
		Assert.notNull(rightColumnOrExpression, "Right expression must not be null");

		return new OperatorExpression(leftColumnOrExpression, operator, rightColumnOrExpression);
	}

	/**
	 * Creates a new {@link OperatorExpression} from simple {@literal StringP} arguments
	 *
	 * @param unqualifiedColumnName gets turned in a {@link Expressions#just(String)} and is expected to be an unqualified
	 *          unique column name but also could be an verbatim expression. Must not be {@literal null}.
	 * @param operator must not be {@literal null}.
	 * @param rightValue is considered a {@link Literal}. Must not be {@literal null}.
	 * @return a new {@literal Comparison} of the first with the third argument using the second argument as comparison
	 *         operator. Guaranteed to be not {@literal null}.
	 * @since 2.3
	 */
	public static OperatorExpression create(String unqualifiedColumnName, String operator, Object rightValue) {

		Assert.notNull(unqualifiedColumnName, "UnqualifiedColumnName must not be null");
		Assert.notNull(operator, "Comparator must not be null");
		Assert.notNull(rightValue, "RightValue must not be null");

		return new OperatorExpression(Expressions.just(unqualifiedColumnName), operator, SQL.literalOf(rightValue));
	}

	/**
	 * @return the left {@link Expression}.
	 */
	public Expression getLeft() {
		return left;
	}

	/**
	 * @return the operator.
	 */
	public String getOperator() {
		return operator;
	}

	/**
	 * @return the right {@link Expression}.
	 */
	public Expression getRight() {
		return right;
	}

	@Override
	public String toString() {
		return left + " " + operator + " " + right;
	}

}
