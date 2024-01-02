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
 * BETWEEN {@link Condition} comparing between {@link Expression}s.
 * <p>
 * Results in a rendered condition: {@code <left> BETWEEN <begin> AND <end>}.
 * </p>
 * 
 * @author Mark Paluch
 * @author Meng Zuozhu
 * @since 2.2
 */
public class Between extends AbstractSegment implements Condition {

	private final Expression column;
	private final Expression begin;
	private final Expression end;
	private final boolean negated;

	private Between(Expression column, Expression begin, Expression end, boolean negated) {

		super(column, begin, end);

		this.column = column;
		this.begin = begin;
		this.end = end;
		this.negated = negated;
	}

	/**
	 * Creates a new {@link Between} {@link Condition} given two {@link Expression}s.
	 *
	 * @param columnOrExpression left side of the comparison.
	 * @param begin begin value of the comparison.
	 * @param end end value of the comparison.
	 * @return the {@link Between} condition.
	 */
	public static Between create(Expression columnOrExpression, Expression begin, Expression end) {

		Assert.notNull(columnOrExpression, "Column or expression must not be null");
		Assert.notNull(begin, "Begin value must not be null");
		Assert.notNull(end, "end value must not be null");

		return new Between(columnOrExpression, begin, end, false);
	}

	/**
	 * @return the column {@link Expression}.
	 */
	public Expression getColumn() {
		return column;
	}

	/**
	 * @return the begin {@link Expression}.
	 */
	public Expression getBegin() {
		return begin;
	}

	/**
	 * @return the end {@link Expression}.
	 */
	public Expression getEnd() {
		return end;
	}

	public boolean isNegated() {
		return negated;
	}

	@Override
	public Between not() {
		return new Between(this.column, this.begin, this.end, !negated);
	}

	@Override
	public String toString() {
		return column + (negated ? " NOT" : "") + " BETWEEN " + begin + " AND " + end;
	}
}
