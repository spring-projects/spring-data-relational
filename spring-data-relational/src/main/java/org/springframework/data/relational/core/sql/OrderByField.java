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

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.NullHandling;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 */
public class OrderByField extends AbstractSegment implements Segment {

	private final Expression expression;
	private final @Nullable Sort.Direction direction;
	private final Sort.NullHandling nullHandling;

	OrderByField(Expression expression, Direction direction, NullHandling nullHandling) {

		Assert.notNull(expression, "Order by expression must not be null");
		Assert.notNull(nullHandling, "NullHandling by expression must not be null");

		this.expression = expression;
		this.direction = direction;
		this.nullHandling = nullHandling;
	}

	public static OrderByField from(Column column) {
		return new OrderByField(column, null, NullHandling.NATIVE);
	}

	public static OrderByField create(String name) {
		return new OrderByField(Column.create(name), null, NullHandling.NATIVE);
	}

	public static OrderByField index(int index) {
		return new OrderByField(new IndexedOrderByField(index), null, NullHandling.NATIVE);
	}

	public OrderByField asc() {
		return new OrderByField(expression, Direction.ASC, NullHandling.NATIVE);
	}

	public OrderByField desc() {
		return new OrderByField(expression, Direction.DESC, NullHandling.NATIVE);
	}

	public OrderByField withNullHandling(NullHandling nullHandling) {
		return new OrderByField(expression, direction, nullHandling);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.relational.core.sql.Visitable#visit(org.springframework.data.relational.core.sql.Visitor)
	 */
	@Override
	public void visit(Visitor visitor) {

		visitor.enter(this);
		expression.visit(visitor);
		visitor.leave(this);
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

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return direction != null ? expression.toString() + " " + direction : expression.toString();
	}

	static class IndexedOrderByField extends Column implements Expression {

		IndexedOrderByField(int index) {
			super("" + index, null);
		}
	}
}
