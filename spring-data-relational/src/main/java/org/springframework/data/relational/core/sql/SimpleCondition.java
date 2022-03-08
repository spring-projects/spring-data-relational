/*
 * Copyright 2019-2022 the original author or authors.
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

/**
 * Simple condition consisting of {@link Expression}, {@code comparator} and {@code predicate}.
 *
 * @author Mark Paluch
 * @since 1.1
 * @deprecated since 2.2.5 use {@link Comparison} instead.
 */
@Deprecated
public class SimpleCondition extends AbstractSegment implements Condition {

	private final Expression expression;

	private final String comparator;

	private final String predicate;

	SimpleCondition(Expression expression, String comparator, String predicate) {

		super(expression);

		this.expression = expression;
		this.comparator = comparator;
		this.predicate = predicate;
	}

	/**
	 * Creates a simple {@link Condition} given {@code column}, {@code comparator} and {@code predicate}.
	 */
	public static SimpleCondition create(String column, String comparator, String predicate) {
		return new SimpleCondition(new Column(column, null), comparator, predicate);
	}

	/**
	 * @return the condition expression (left-hand-side)
	 * @since 2.0
	 */
	public Expression getExpression() {
		return expression;
	}

	/**
	 * @return the comparator.
	 */
	public String getComparator() {
		return comparator;
	}

	/**
	 * @return the condition predicate (right-hand-side)
	 */
	public String getPredicate() {
		return predicate;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return expression + " " + comparator + " " + predicate;
	}
}
