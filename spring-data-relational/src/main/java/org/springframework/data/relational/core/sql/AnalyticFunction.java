/*
 * Copyright 2021-2025 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;

/**
 * Represents an analytic function, also known as windowing function
 * 
 * @author Jens Schauder
 * @since 2.7
 */
public class AnalyticFunction extends AbstractSegment implements Expression {

	private final SimpleFunction function;
	private final Partition partition;
	private final OrderBy orderBy;

	public static AnalyticFunction create(String function, Expression... arguments) {

		return new AnalyticFunction(SimpleFunction.create(function, Arrays.asList(arguments)), new Partition(),
				new OrderBy());
	}

	private AnalyticFunction(SimpleFunction function, Partition partition, OrderBy orderBy) {

		super(function, partition, orderBy);

		this.function = function;
		this.partition = partition;
		this.orderBy = orderBy;
	}

	/**
	 * Specify the {@literal PARTITION BY} clause of an analytic function
	 * 
	 * @param partitionBy Typically, column but other expressions are fine to.
	 * @return a new {@literal AnalyticFunction} is partitioned by the given expressions, overwriting any expression
	 *         previously present.
	 */
	public AnalyticFunction partitionBy(Expression... partitionBy) {
		return new AnalyticFunction(function, new Partition(partitionBy), orderBy);
	}

	/**
	 * Specify the {@literal PARTITION BY} clause of an analytic function
	 * 
	 * @param partitionBy Typically, column but other expressions are fine to.
	 * @return a new {@literal AnalyticFunction} is partitioned by the given expressions, overwriting any expression
	 *         previously present.
	 * @since 3.5
	 */
	public AnalyticFunction partitionBy(Collection<? extends Expression> partitionBy) {
		return partitionBy(partitionBy.toArray(new Expression[0]));
	}

	/**
	 * Specify the {@literal ORDER BY} clause of an analytic function
	 * 
	 * @param orderBy Typically, column but other expressions are fine to.
	 * @return a new {@literal AnalyticFunction} is ordered by the given expressions, overwriting any expression
	 *         previously present.
	 */
	public AnalyticFunction orderBy(OrderByField... orderBy) {
		return new AnalyticFunction(function, partition, new OrderBy(orderBy));
	}

	/**
	 * Specify the {@literal ORDER BY} clause of an analytic function
	 * 
	 * @param orderBy Typically, column but other expressions are fine to.
	 * @return a new {@literal AnalyticFunction} is ordered by the given expressions, overwriting any expression
	 *         previously present.
	 * @since 3.5
	 */
	public AnalyticFunction orderBy(Collection<? extends Expression> orderBy) {
		return orderBy(orderBy.toArray(new Expression[0]));
	}

	/**
	 * Specify the {@literal ORDER BY} clause of an analytic function
	 * 
	 * @param orderBy array of {@link Expression}. Typically, column but other expressions are fine to.
	 * @return a new {@literal AnalyticFunction} is ordered by the given expressions, overwriting any expression
	 *         previously present.
	 */
	public AnalyticFunction orderBy(Expression... orderBy) {

		final OrderByField[] orderByFields = Arrays.stream(orderBy) //
				.map(OrderByField::from) //
				.toArray(OrderByField[]::new);

		return new AnalyticFunction(function, partition, new OrderBy(orderByFields));
	}

	public AliasedAnalyticFunction as(String alias) {
		return new AliasedAnalyticFunction(this, SqlIdentifier.unquoted(alias));
	}

	public AliasedAnalyticFunction as(SqlIdentifier alias) {
		return new AliasedAnalyticFunction(this, alias);
	}

	public static class Partition extends SegmentList<Expression> {
		Partition(Expression... expressions) {
			super(expressions);
		}
	}

	private static class AliasedAnalyticFunction extends AnalyticFunction implements Aliased {

		private final SqlIdentifier alias;

		AliasedAnalyticFunction(AnalyticFunction analyticFunction, SqlIdentifier alias) {

			super(analyticFunction.function, analyticFunction.partition, analyticFunction.orderBy);
			this.alias = alias;
		}

		@Override
		public SqlIdentifier getAlias() {
			return alias;
		}
	}
}
