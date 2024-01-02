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

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.NullHandling;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Represents a field in the {@code ORDER BY} clause.
 *
 * @author Mark Paluch
 * @author Milan Milanov
 * @since 1.1
 */
public class OrderByField extends AbstractSegment {

	private final Expression expression;
	private final @Nullable Sort.Direction direction;
	private final Sort.NullHandling nullHandling;

	private OrderByField(Expression expression, @Nullable Direction direction, NullHandling nullHandling) {

		super(expression);
		Assert.notNull(expression, "Order by expression must not be null");
		Assert.notNull(nullHandling, "NullHandling by expression must not be null");

		this.expression = expression;
		this.direction = direction;
		this.nullHandling = nullHandling;
	}

	/**
	 * Creates a new {@link OrderByField} from an {@link Expression} applying default ordering.
	 *
	 * @param expression must not be {@literal null}.
	 * @return the {@link OrderByField}.
	 */
	public static OrderByField from(Expression expression) {
		return new OrderByField(expression, null, NullHandling.NATIVE);
	}

	/**
	 * Creates a new {@link OrderByField} from an {@link Expression} applying a given ordering.
	 *
	 * @param expression must not be {@literal null}.
	 * @param direction order direction
	 * @return the {@link OrderByField}.
	 */
	public static OrderByField from(Expression expression, Direction direction) {
		return new OrderByField(expression, direction, NullHandling.NATIVE);
	}

	/**
	 * Creates a new {@link OrderByField} from a the current one using ascending sorting.
	 *
	 * @return the new {@link OrderByField} with ascending sorting.
	 * @see #desc()
	 */
	public OrderByField asc() {
		return new OrderByField(expression, Direction.ASC, nullHandling);
	}

	/**
	 * Creates a new {@link OrderByField} from a the current one using descending sorting.
	 *
	 * @return the new {@link OrderByField} with descending sorting.
	 * @see #asc()
	 */
	public OrderByField desc() {
		return new OrderByField(expression, Direction.DESC, nullHandling);
	}

	/**
	 * Creates a new {@link OrderByField} with {@link NullHandling} applied.
	 *
	 * @param nullHandling must not be {@literal null}.
	 * @return the new {@link OrderByField} with {@link NullHandling} applied.
	 */
	public OrderByField withNullHandling(NullHandling nullHandling) {
		return new OrderByField(expression, direction, nullHandling);
	}

	public Expression getExpression() {
		return expression;
	}

	@Nullable
	public Direction getDirection() {
		return direction;
	}

	public NullHandling getNullHandling() {
		return nullHandling;
	}

	@Override
	public String toString() {
		return direction != null ? expression + " " + direction : expression.toString();
	}
}
