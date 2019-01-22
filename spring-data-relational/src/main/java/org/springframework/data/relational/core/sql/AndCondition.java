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
 * {@link Condition} representing an {@code AND} relation between two {@link Condition}s.
 *
 * @author Mark Paluch
 * @see Condition#and(Condition)
 */
public class AndCondition implements Condition {

	private final Condition left;
	private final Condition right;

	AndCondition(Condition left, Condition right) {
		this.left = left;
		this.right = right;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Visitable#visit(org.springframework.data.relational.core.sql.Visitor)
	 */
	@Override
	public void visit(Visitor visitor) {

		Assert.notNull(visitor, "Visitor must not be null!");

		visitor.enter(this);
		left.visit(visitor);
		right.visit(visitor);
		visitor.leave(this);
	}

	/**
	 * @return the left {@link Condition}.
	 */
	public Condition getLeft() {
		return left;
	}

	/**
	 * @return the right {@link Condition}.
	 */
	public Condition getRight() {
		return right;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return left.toString() + " AND " + right.toString();
	}
}
