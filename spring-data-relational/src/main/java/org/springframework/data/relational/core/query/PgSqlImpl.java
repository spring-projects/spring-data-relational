/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.relational.core.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.Vector;
import org.springframework.data.relational.core.sql.*;
import org.springframework.lang.Nullable;

/**
 * Implementation of the Postgres dialect for criteria and query expressions.
 *
 * @author Mark Paluch
 * @since 4.0
 */
class PgSqlImpl {

	static class DefaultPostgresCriteriaStep extends Criteria.DefaultCriteriaStep
			implements PgSql.PgCriteria.PostgresCriteriaStep {

		private final QueryExpression source;

		protected DefaultPostgresCriteriaStep(String propertyName) {
			this(CriteriaSource.ofDotPath(propertyName));
		}

		protected DefaultPostgresCriteriaStep(QueryExpression source) {
			super(SqlIdentifier.unquoted("foo"));
			this.source = source;
		}

		protected DefaultPostgresCriteriaStep(CriteriaSources.DotPath identifier) {
			super(SqlIdentifier.unquoted(identifier.name()));
			this.source = identifier;
		}

		protected DefaultPostgresCriteriaStep(CriteriaSources.SqlIdentifierSource identifier) {
			super(identifier.identifier());
			this.source = identifier;
		}

		protected DefaultPostgresCriteriaStep(SqlIdentifier sqlIdentifier) {
			super(sqlIdentifier);
			this.source = CriteriaSource.of(sqlIdentifier);
		}

		protected Criteria createCriteria(CriteriaDefinition.Comparator comparator, @Nullable Object value) {

			return new PgSql.PgCriteria(new PostgresComparison(source,
					(comparator == CriteriaDefinition.Comparator.IS_TRUE || comparator == CriteriaDefinition.Comparator.IS_FALSE)
							? "="
							: comparator.getComparator(),
					new ValueExpression(value)));
		}

		protected PgSql.PgCriteria createCriteria(String operator, Object value) {
			return createCriteria(new PostgresComparison(source, operator,
					value instanceof QueryExpression e ? e : new ValueExpression(value)));
		}

		protected PgSql.PgCriteria createCriteria(QueryExpression queryExpression) {
			return new PgSql.PgCriteria(queryExpression);
		}

		@Override
		public PgSql.PgCriteria exists(Object value) {
			return createCriteria("?", value);
		}

		@Override
		public PgSql.PgCriteria contains(Object value) {
			return createCriteria("@>", value);
		}

		@Override
		public PgSql.PgCriteria overlaps(Object value) {
			return createCriteria("?", value);
		}

		@Override
		public PgSql.PgCriteria.PostgresJsonCriteriaStep json() {
			return new PgSql.PgCriteria.PostgresJsonCriteriaStep() {

				@Override
				public PgSql.PgCriteria contains(String field) {
					return createCriteria("?", new ValueExpression(field));
				}

				@Override
				public PgSql.PgCriteria contains(Object value) {
					return createCriteria("?", value);
				}

				@Override
				public PgSql.PgCriteria containsAll(Iterable<Object> values) {
					return createCriteria("?&", new ArrayExpression(values));
				}

				@Override
				public PgSql.PgCriteria containsAll(Object... values) {
					return createCriteria("?&", new ArrayExpression(Arrays.asList(values)));
				}

				@Override
				public PgSql.PgCriteria containsAny(Iterable<Object> values) {
					return createCriteria("?|", new ArrayExpression(values));
				}

				@Override
				public PgSql.PgCriteria containsAny(Object... values) {
					return createCriteria("?|", new ArrayExpression(Arrays.asList(values)));
				}

				@Override
				public PgSql.PgCriteria jsonPathMatches(String jsonPath) {
					return createCriteria("@?", new ValueExpression(jsonPath));
				}

				@Override
				public PgSql.PgCriteria jsonPath(String jsonPath) {
					return createCriteria("@@", new ValueExpression(jsonPath));
				}
			};
		}
	}

	private record PostgresComparison(QueryExpression lhs, String operator,
			QueryExpression rhs) implements QueryExpression {

		@Override
		public QueryRenderContext contextualize(QueryRenderContext context) {
			return this.rhs.contextualize(this.lhs.contextualize(context));
		}

		@Override
		public Expression render(QueryRenderContext context) {

			Expression lhs = this.lhs.render(context);
			Expression rhs = this.rhs.render(context);

			return Comparison.create(lhs, operator, rhs);
		}

	}

	private record ValueExpression(Object value) implements QueryExpression {

		@Override
		public Expression render(QueryRenderContext context) {
			return context.bind(value);
		}
	}

	static class ValuesExpression implements QueryExpression {

		private final Iterable<Object> values;

		public ValuesExpression(Iterable<Object> values) {
			this.values = values;
		}

		@Override
		public Expression render(QueryRenderContext context) {

			List<BindMarker> bindMarkers = new ArrayList<>();
			for (Object value : this.values) {
				bindMarkers.add(context.bind(value));
			}

			return TupleExpression.create(bindMarkers);
		}
	}

	record FunctionExpression(String function, Iterable<Object> values) implements PgSql.PostgresQueryExpression {

		@Override
		public Expression render(QueryRenderContext context) {
			return SimpleFunction.create(function, createArgumentExpressions(values(), context));
		}

		private static List<Expression> createArgumentExpressions(Iterable<Object> values, QueryRenderContext context) {

			List<Expression> arguments = new ArrayList<>();
			for (Object value : values) {
				if (value == null) {
					arguments.add(Expressions.just("NULL"));
				} else if (value instanceof QueryExpression qe) {
					arguments.add(qe.render(context));
				} else {
					arguments.add(context.bind(value));
				}
			}
			return arguments;
		}
	}

	record ArrayExpression(Iterable<Object> values) implements QueryExpression {

		@Override
		public Expression render(QueryRenderContext context) {
			return BaseFunction.create("array", "[", "]", FunctionExpression.createArgumentExpressions(values(), context));
		}
	}

	record DefaultFunctions(CriteriaSource source) implements PgSql.Functions {

		@Override
		public PgSql.PostgresQueryExpression index(int index) {
			return new JsonIndexFunction(source, JsonIndexFunction.FIELD_OR_INDEX, index);
		}

		@Override
		public PgSql.PostgresQueryExpression field(String field) {
			return new JsonIndexFunction(source, JsonIndexFunction.FIELD_OR_INDEX, field);
		}

		@Override
		public PgSql.PostgresQueryExpression distanceTo(Vector vector,
				Function<Distances, ScoringFunction> distanceFunction) {
			return new DistanceFunction(source, distanceFunction.apply(new Distances() {}), vector);
		}
	}

	record DistanceFunction(CriteriaSource source, ScoringFunction scoringFunction,
			Vector vector) implements PgSql.PostgresQueryExpression {

		@Override
		public QueryRenderContext contextualize(QueryRenderContext context) {
			return source.contextualize(context);
		}

		@Override
		public Expression render(QueryRenderContext context) {

			String operator = getOperator();

			return OperatorExpression.create(source.render(context), operator, context.bind(vector));
		}

		private String getOperator() {

			if (scoringFunction == ScoringFunction.cosine()) {
				return "<=>";
			} else if (scoringFunction == ScoringFunction.euclidean()) {
				return "<->";
			} else if (scoringFunction == ScoringFunction.dotProduct()) {
				return "<#>";
			}

			return scoringFunction.getName();
		}
	}

	static class JsonIndexFunction implements PgSql.PostgresQueryExpression {

		public static final String FIELD_OR_INDEX = "-";
		public static final String PATH = "#";

		private final CriteriaSource source;
		private final String baseOperator;
		private final boolean asString;
		private final Object keyOrIndex;

		public JsonIndexFunction(CriteriaSource source, String baseOperator, Object keyOrIndex) {
			this(source, baseOperator, false, keyOrIndex);
		}

		public JsonIndexFunction(CriteriaSource source, String baseOperator, boolean asString, Object keyOrIndex) {
			this.source = source;
			this.baseOperator = baseOperator;
			this.asString = asString;
			this.keyOrIndex = keyOrIndex;
		}

		@Override
		public PgSql.PostgresQueryExpression asString() {
			return new JsonIndexFunction(this.source, this.baseOperator, true, this.keyOrIndex);
		}

		@Override
		public PgSql.PostgresQueryExpression asJson() {
			return this;
		}

		@Override
		public PgSql.PostgresQueryExpression asJsonb() {
			return this;
		}

		@Override
		public PgSql.PostgresQueryExpression as(String type) {
			return new AppendingPostgresExpression(this.nest(), "::" + type);
		}

		@Override
		public QueryRenderContext contextualize(QueryRenderContext context) {
			return source.contextualize(context);
		}

		@Override
		public Expression render(QueryRenderContext context) {
			return OperatorExpression.create(source.render(context), baseOperator + (asString ? ">>" : ">"),
					context.bind(keyOrIndex));
		}
	}

	record AppendingPostgresExpression(QueryExpression source, String appendix) implements PgSql.PostgresQueryExpression {

		@Override
		public QueryRenderContext contextualize(QueryRenderContext context) {
			return source.contextualize(context);
		}

		@Override
		public Expression render(QueryRenderContext context) {
			return new PostfixExpression(source.render(context), Expressions.just(appendix));
		}
	}

	record ArrayIndexPostgresExpression(QueryExpression source,
			Object keyOrIndex) implements PgSql.PostgresQueryExpression {

		@Override
		public Expression render(QueryRenderContext context) {
			return new PostfixExpression(source.render(context), new ArrayIndexExpression(context.bind(keyOrIndex)));
		}
	}

}
