/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
 * <p/>
 * Results in a rendered condition: {@code <left> LIKE <right>}.
 *
 * @author Mark Paluch
 */
public class Like extends AbstractSegment implements Condition {

	private final Expression left;
	private final Expression right;

	private Like(Expression left, Expression right) {

		super(left, right);

		this.left = left;
		this.right = right;
	}

	/**
	 * Creates a new {@link Like} {@link Condition} given two {@link Expression}s.
	 *
	 * @param leftColumnOrExpression the left {@link Expression}.
	 * @param rightColumnOrExpression the right {@link Expression}.
	 * @return the {@link Like} condition.
	 */
	public static Like create(Expression leftColumnOrExpression, Expression rightColumnOrExpression) {

		Assert.notNull(leftColumnOrExpression, "Left expression must not be null!");
		Assert.notNull(rightColumnOrExpression, "Right expression must not be null!");

		return new Like(leftColumnOrExpression, rightColumnOrExpression);
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

	@Override
	public String toString() {
		return left.toString() + " LIKE " + right.toString();
	}
}
