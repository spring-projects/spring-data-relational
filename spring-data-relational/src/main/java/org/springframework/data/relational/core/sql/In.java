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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * {@code IN} {@link Condition} clause.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 1.1
 */
public class In extends AbstractSegment implements Condition {

	private final Expression left;
	private final Collection<Expression> expressions;
	private final boolean notIn;

	private In(Expression left, Collection<Expression> expressions, boolean notIn) {

		super(toArray(left, expressions));

		this.left = left;
		this.expressions = expressions;
		this.notIn = notIn;
	}

	private static Segment[] toArray(Expression expression, Collection<Expression> expressions) {

		Segment[] segments = new Segment[1 + expressions.size()];
		segments[0] = expression;

		int index = 1;

		for (Expression e : expressions) {
			segments[index++] = e;
		}

		return segments;
	}

	/**
	 * Creates a new {@link In} {@link Condition} given left and right {@link Expression}s.
	 *
	 * @param columnOrExpression left hand side of the {@link Condition} must not be {@literal null}.
	 * @param arg right hand side (collection {@link Expression}) must not be {@literal null}.
	 * @return the {@link In} {@link Condition}.
	 */
	public static In create(Expression columnOrExpression, Expression arg) {

		Assert.notNull(columnOrExpression, "Comparison column or expression must not be null");
		Assert.notNull(arg, "Expression argument must not be null");

		return new In(columnOrExpression, Collections.singletonList(arg), false);
	}

	/**
	 * Creates a new {@link In} {@link Condition} given left and right {@link Expression}s.
	 *
	 * @param columnOrExpression left hand side of the {@link Condition} must not be {@literal null}.
	 * @param expressions right hand side (collection {@link Expression}) must not be {@literal null}.
	 * @return the {@link In} {@link Condition}.
	 */
	public static In create(Expression columnOrExpression, Collection<? extends Expression> expressions) {

		Assert.notNull(columnOrExpression, "Comparison column or expression must not be null");
		Assert.notNull(expressions, "Expression argument must not be null");

		return new In(columnOrExpression, new ArrayList<>(expressions), false);
	}

	/**
	 * Creates a new {@link In} {@link Condition} given left and right {@link Expression}s.
	 *
	 * @param columnOrExpression left hand side of the {@link Condition} must not be {@literal null}.
	 * @param expressions right hand side (collection {@link Expression}) must not be {@literal null}.
	 * @return the {@link In} {@link Condition}.
	 */
	public static In create(Expression columnOrExpression, Expression... expressions) {

		Assert.notNull(columnOrExpression, "Comparison column or expression must not be null");
		Assert.notNull(expressions, "Expression argument must not be null");

		return new In(columnOrExpression, Arrays.asList(expressions), false);
	}

	/**
	 * Creates a new {@link In} {@link Condition} given left and right {@link Expression}s.
	 *
	 * @param columnOrExpression left hand side of the {@link Condition} must not be {@literal null}.
	 * @param arg right hand side (collection {@link Expression}) must not be {@literal null}.
	 * @return the {@link In} {@link Condition}.
	 */
	public static In createNotIn(Expression columnOrExpression, Expression arg) {

		Assert.notNull(columnOrExpression, "Comparison column or expression must not be null");
		Assert.notNull(arg, "Expression argument must not be null");

		return new In(columnOrExpression, Collections.singletonList(arg), true);
	}

	/**
	 * Creates a new {@link In} {@link Condition} given left and right {@link Expression}s.
	 *
	 * @param columnOrExpression left hand side of the {@link Condition} must not be {@literal null}.
	 * @param expressions right hand side (collection {@link Expression}) must not be {@literal null}.
	 * @return the {@link In} {@link Condition}.
	 */
	public static In createNotIn(Expression columnOrExpression, Collection<? extends Expression> expressions) {

		Assert.notNull(columnOrExpression, "Comparison column or expression must not be null");
		Assert.notNull(expressions, "Expression argument must not be null");

		return new In(columnOrExpression, new ArrayList<>(expressions), true);
	}

	/**
	 * Creates a new {@link In} {@link Condition} given left and right {@link Expression}s.
	 *
	 * @param columnOrExpression left hand side of the {@link Condition} must not be {@literal null}.
	 * @param expressions right hand side (collection {@link Expression}) must not be {@literal null}.
	 * @return the {@link In} {@link Condition}.
	 */
	public static In createNotIn(Expression columnOrExpression, Expression... expressions) {

		Assert.notNull(columnOrExpression, "Comparison column or expression must not be null");
		Assert.notNull(expressions, "Expression argument must not be null");

		return new In(columnOrExpression, Arrays.asList(expressions), true);
	}

	@Override
	public Condition not() {
		return new In(left, expressions, !notIn);
	}

	/**
	 * @return {@code true} if this condition has at least one expression.
	 * @since 2.1
	 */
	public boolean hasExpressions() {
		return !expressions.isEmpty();
	}

	@Override
	public String toString() {

		if (hasExpressions()) {
			return left + (notIn ? " NOT" : "") + " IN (" + StringUtils.collectionToDelimitedString(expressions, ", ") + ")";
		}

		return notIn ? TrueCondition.INSTANCE.toString() : FalseCondition.INSTANCE.toString();
	}

	public boolean isNotIn() {
		return notIn;
	}
}
