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
 * Comparing {@link Condition} comparing two {@link Expression}s.
 * <p>
 * Results in a rendered condition: {@code <left> <comparator> <right>} (e.g. {@code col = 'predicate'}.
 * </p>
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 1.1
 */
public class Comparison extends AbstractSegment implements Condition {

	private final Expression left;
	private final String comparator;
	private final Expression right;

	private Comparison(Expression left, String comparator, Expression right) {

		super(left, right);

		this.left = left;
		this.comparator = comparator;
		this.right = right;
	}

	/**
	 * Creates a new {@link Comparison} {@link Condition} given two {@link Expression}s.
	 *
	 * @param leftColumnOrExpression the left {@link Expression}.
	 * @param comparator the comparator.
	 * @param rightColumnOrExpression the right {@link Expression}.
	 * @return the {@link Comparison} condition.
	 */
	public static Comparison create(Expression leftColumnOrExpression, String comparator,
			Expression rightColumnOrExpression) {

		Assert.notNull(leftColumnOrExpression, "Left expression must not be null");
		Assert.notNull(comparator, "Comparator must not be null");
		Assert.notNull(rightColumnOrExpression, "Right expression must not be null");

		return new Comparison(leftColumnOrExpression, comparator, rightColumnOrExpression);
	}

	/**
	 * Creates a new {@link Comparison} from simple {@literal StringP} arguments
	 *
	 * @param unqualifiedColumnName gets turned in a {@link Expressions#just(String)} and is expected to be an unqualified
	 *          unique column name but also could be an verbatim expression. Must not be {@literal null}.
	 * @param comparator must not be {@literal null}.
	 * @param rightValue is considered a {@link Literal}. Must not be {@literal null}.
	 * @return a new {@literal Comparison} of the first with the third argument using the second argument as comparison
	 *         operator. Guaranteed to be not {@literal null}.
	 * @since 2.3
	 */
	public static Comparison create(String unqualifiedColumnName, String comparator, Object rightValue) {

		Assert.notNull(unqualifiedColumnName, "UnqualifiedColumnName must not be null");
		Assert.notNull(comparator, "Comparator must not be null");
		Assert.notNull(rightValue, "RightValue must not be null");

		return new Comparison(Expressions.just(unqualifiedColumnName), comparator, SQL.literalOf(rightValue));
	}

	@Override
	public Condition not() {

		if ("=".equals(comparator)) {
			return new Comparison(left, "!=", right);
		}

		if ("!=".equals(comparator)) {
			return new Comparison(left, "=", right);
		}

		return new Not(this);
	}

	/**
	 * @return the left {@link Expression}.
	 */
	public Expression getLeft() {
		return left;
	}

	/**
	 * @return the comparator.
	 */
	public String getComparator() {
		return comparator;
	}

	/**
	 * @return the right {@link Expression}.
	 */
	public Expression getRight() {
		return right;
	}

	@Override
	public String toString() {
		return left + " " + comparator + " " + right;
	}
}
