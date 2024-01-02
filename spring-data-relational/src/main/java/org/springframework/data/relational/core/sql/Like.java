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
 * LIKE {@link Condition} comparing two {@link Expression}s.
 * <p>
 * Results in a rendered condition: {@code <left> LIKE <right>}.
 * </p>
 * @author Mark Paluch
 * @author Meng Zuozhu
 * @since 1.1
 */
public class Like extends AbstractSegment implements Condition {

	private final Expression left;
	private final Expression right;
	private final boolean negated;

	private Like(Expression left, Expression right, boolean negated) {

		super(left, right);

		this.left = left;
		this.right = right;
		this.negated = negated;
	}

	/**
	 * Creates a new {@link Like} {@link Condition} given two {@link Expression}s.
	 *
	 * @param leftColumnOrExpression the left {@link Expression}.
	 * @param rightColumnOrExpression the right {@link Expression}.
	 * @return the {@link Like} condition.
	 */
	public static Like create(Expression leftColumnOrExpression, Expression rightColumnOrExpression) {

		Assert.notNull(leftColumnOrExpression, "Left expression must not be null");
		Assert.notNull(rightColumnOrExpression, "Right expression must not be null");

		return new Like(leftColumnOrExpression, rightColumnOrExpression, false);
	}

	/**
	 * @return the left {@link Expression}.
	 */
	public Expression getLeft() {
		return left;
	}

	/**
	 * @return the right {@link Expression}.
	 */
	public Expression getRight() {
		return right;
	}

	public boolean isNegated() {
		return negated;
	}

	@Override
	public Like not() {
		return new Like(this.left, this.right, !negated);
	}

	@Override
	public String toString() {
		return left + (negated ? " NOT" : "") + " LIKE " + right;
	}
}
