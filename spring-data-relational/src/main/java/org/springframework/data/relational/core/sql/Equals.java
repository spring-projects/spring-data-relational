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
 * Equals to {@link Condition} comparing two {@link Expression}s.
 * <p/>
 * Results in a rendered condition: {@code <left> = <right>}.
 *
 * @author Mark Paluch
 */
public class Equals extends AbstractSegment implements Condition {

	private final Expression left;
	private final Expression right;

	Equals(Expression left, Expression right) {

		super(left, right);

		this.left = left;
		this.right = right;
	}

	/**
	 * Creates a new {@link Equals} {@link Condition} given two {@link Expression}s.
	 *
	 * @param left the left {@link Expression}.
	 * @param right the right {@link Expression}.
	 * @return the {@link Equals} condition.
	 */
	public static Equals create(Expression left, Expression right) {

		Assert.notNull(left, "Left expression must not be null!");
		Assert.notNull(right, "Right expression must not be null!");

		return new Equals(left, right);
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
		return left.toString() + " = " + right.toString();
	}
}
