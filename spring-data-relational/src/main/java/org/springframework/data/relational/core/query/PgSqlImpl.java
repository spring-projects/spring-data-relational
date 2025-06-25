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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.Vector;
import org.springframework.data.relational.core.sql.ArrayIndexExpression;
import org.springframework.data.relational.core.sql.BaseFunction;
import org.springframework.data.relational.core.sql.BindMarker;
import org.springframework.data.relational.core.sql.Comparison;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Expressions;
import org.springframework.data.relational.core.sql.OperatorExpression;
import org.springframework.data.relational.core.sql.PostfixExpression;
import org.springframework.data.relational.core.sql.SimpleFunction;
import org.springframework.data.relational.core.sql.TupleExpression;
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

		protected DefaultPostgresCriteriaStep(String propertyName) {
			this(QueryExpression.column(propertyName));
		}

		protected DefaultPostgresCriteriaStep(QueryExpression source) {
			super(source);
		}

		protected Criteria createCriteria(CriteriaDefinition.Comparator comparator, @Nullable Object value) {

			return new PgSql.PgCriteria(new PostgresComparison(getLhs(),
					(comparator == CriteriaDefinition.Comparator.IS_TRUE || comparator == CriteriaDefinition.Comparator.IS_FALSE)
							? "="
							: comparator.getComparator(),
					new ValueExpression(value)));
		}

		protected PgSql.PgCriteria createCriteria(String operator, Object value) {
			return createCriteria(new PostgresComparison(getLhs(), operator,
					value instanceof QueryExpression e ? e : new ValueExpression(value)));
		}

		protected PgSql.PgCriteria createCriteria(QueryExpression queryExpression) {
			return new PgSql.PgCriteria(queryExpression);
		}

		@Override
		public PgSql.PgCriteria.PostgresArrayCriteriaStep arrays() {
			return new PgSql.PgCriteria.PostgresArrayCriteriaStep() {
				@Override
				public PgSql.PgCriteria contains(QueryExpression expression) {
					return createCriteria("@>", expression);
				}

				@Override
				public PgSql.PgCriteria overlaps(QueryExpression expression) {
					return createCriteria("&&", expression);
				}
			};
		}

		@Override
		public PgSql.PgCriteria.PostgresJsonCriteriaStep json() {
			return new PgSql.PgCriteria.PostgresJsonCriteriaStep() {

				@Override
				public PgSql.PgCriteria exists(QueryExpression expression) {
					return createCriteria("?", expression);
				}

				@Override
				public PgSql.PgCriteria contains(Object value) {
					return createCriteria("@>", value);
				}

				@Override
				public PgSql.PgCriteria containsAll(Iterable<Object> values) {
					return createCriteria("?&", ArrayExpression.expressionOrWrap(values));
				}

				@Override
				public PgSql.PgCriteria containsAny(Iterable<Object> values) {
					return createCriteria("?|", ArrayExpression.expressionOrWrap(values));
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
			QueryExpression rhs) implements PgSql.PostgresQueryExpression {

		@Override
		public ExpressionTypeContext getType(EvaluationContext context) {
			return ExpressionTypeContext.bool();
		}

		@Override
		public Expression evaluate(EvaluationContext context) {

			Expression lhs = this.lhs.evaluate(context.withType(this.rhs));
			Expression rhs = this.rhs.evaluate(context.withType(this.lhs));

			return Comparison.create(lhs, operator, rhs);
		}

	}

	record ValueExpression(Object value) implements QueryExpression {

		@Override
		public Expression evaluate(EvaluationContext context) {
			return context.bind(value);
		}
	}

	static class ValuesExpression implements QueryExpression {

		private final Iterable<Object> values;

		public ValuesExpression(Iterable<Object> values) {
			this.values = values;
		}

		public static QueryExpression oneOrMany(Object[] values) {

			if (values.length == 1) {
				return new ValueExpression(values[0]);
			}

			return new ValuesExpression(Arrays.asList(values));
		}

		@Override
		public Expression evaluate(EvaluationContext context) {

			List<BindMarker> bindMarkers = new ArrayList<>();
			for (Object value : this.values) {
				bindMarkers.add(context.bind(value));
			}

			return TupleExpression.create(bindMarkers);
		}
	}

	record FunctionExpression(String function, Iterable<Object> values) implements PgSql.PostgresQueryExpression {

		@Override
		public Expression evaluate(EvaluationContext context) {
			return SimpleFunction.create(function, createArgumentExpressions(values(), context));
		}

		private static List<Expression> createArgumentExpressions(Iterable<Object> values, EvaluationContext context) {

			List<Expression> arguments = new ArrayList<>();
			for (Object value : values) {
				if (value == null) {
					arguments.add(Expressions.just("NULL"));
				} else if (value instanceof QueryExpression qe) {
					arguments.add(qe.evaluate(context));
				} else {
					arguments.add(context.bind(value));
				}
			}
			return arguments;
		}
	}

	record ArrayExpression(Iterable<Object> values) implements QueryExpression {

		public static QueryExpression expressionOrWrap(Object[] values) {
			return expressionOrWrap(Arrays.asList(values));
		}

		public static QueryExpression expressionOrWrap(Iterable<Object> values) {

			Iterator<Object> iterator = values.iterator();
			if (iterator.hasNext()) {

				Object next = iterator.next();
				if (!iterator.hasNext() && next instanceof QueryExpression queryExpression) {
					return queryExpression;
				}
			}

			return new ArrayExpression(values);
		}

		@Override
		public ExpressionTypeContext getType(EvaluationContext context) {
			return ExpressionTypeContext.object().asCollection();
		}

		@Override
		public Expression evaluate(EvaluationContext context) {
			return BaseFunction.create("array", "[", "]", FunctionExpression.createArgumentExpressions(values(), context));
		}
	}

	record JsonExpression(Map<String, Object> jsonObject, String type) implements QueryExpression {

		@Override
		public Expression evaluate(EvaluationContext context) {
			return new PostfixExpression(context.bind(jsonObject), Expressions.just("::" + type));
		}
	}

	record DefaultOperators(QueryExpression source) implements PgSql.Operators {

		@Override
		public PgSql.PostgresQueryExpression contains(QueryExpression expression) {
			return new ArrayOperator(source, "@>", expression,
					ArrayOperator.just(QueryExpression.ExpressionTypeContext.bool()));
		}

		@Override
		public PgSql.PostgresQueryExpression overlaps(QueryExpression expression) {
			return new ArrayOperator(source, "&&", expression,
					ArrayOperator.just(QueryExpression.ExpressionTypeContext.bool()));
		}

		@Override
		public PgSql.PostgresQueryExpression concatWith(QueryExpression expression) {
			return new ArrayOperator(source, "||", expression, QueryExpression::getType);
		}

		@Override
		public PgSql.PostgresQueryExpression index(int index) {
			return new JsonIndexOperator(source, JsonIndexOperator.FIELD_OR_INDEX, index);
		}

		@Override
		public PgSql.PostgresQueryExpression field(String field) {
			return new JsonIndexOperator(source, JsonIndexOperator.FIELD_OR_INDEX, field);
		}

		@Override
		public PgSql.PostgresQueryExpression distanceTo(Vector vector,
				Function<Distances, ScoringFunction> distanceFunction) {
			return new DistanceFunction(source, distanceFunction.apply(new Distances() {}), vector);
		}
	}

	record DistanceFunction(QueryExpression source, ScoringFunction scoringFunction,
			Vector vector) implements PgSql.PostgresQueryExpression {

		@Override
		public ExpressionTypeContext getType(EvaluationContext context) {
			return ExpressionTypeContext.of(Number.class);
		}

		@Override
		public Expression evaluate(EvaluationContext context) {

			String operator = getOperator();

			return OperatorExpression.create(source.evaluate(context), operator, context.bind(vector));
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

	static class ArrayOperator implements PgSql.PostgresQueryExpression {

		private final QueryExpression lhs;
		private final String operator;
		private final QueryExpression rhs;
		private final TypeFunction typeFunction;

		public ArrayOperator(QueryExpression lhs, String operator, QueryExpression rhs, TypeFunction typeFunction) {
			this.lhs = lhs;
			this.operator = operator;
			this.rhs = rhs;
			this.typeFunction = typeFunction;
		}

		static TypeFunction just(ExpressionTypeContext type) {
			return (ex, context) -> type;
		}

		@Override
		public PgSql.PostgresQueryExpression as(String type) {
			return PgCastExpression.create(this.nest(), type);
		}

		@Override
		public ExpressionTypeContext getType(EvaluationContext context) {

			ExpressionTypeContext l = typeFunction.getType(lhs, context);
			ExpressionTypeContext r = typeFunction.getType(rhs, context);

			return l.getAssignableType(r);
		}

		@Override
		public Expression evaluate(EvaluationContext context) {
			return OperatorExpression.create(lhs.evaluate(context.withType(rhs)), operator,
					rhs.evaluate(context.withType(lhs)));
		}
	}

	static class JsonIndexOperator implements PgSql.PostgresQueryExpression {

		public static final String FIELD_OR_INDEX = "-";
		public static final String PATH = "#";

		private final QueryExpression source;
		private final String baseOperator;
		private final boolean asString;
		private final Object keyOrIndex;

		public JsonIndexOperator(QueryExpression source, String baseOperator, Object keyOrIndex) {
			this(source, baseOperator, false, keyOrIndex);
		}

		public JsonIndexOperator(QueryExpression source, String baseOperator, boolean asString, Object keyOrIndex) {
			this.source = source;
			this.baseOperator = baseOperator;
			this.asString = asString;
			this.keyOrIndex = keyOrIndex;
		}

		@Override
		public PgSql.PostgresQueryExpression asString() {
			return new JsonIndexOperator(this.source, this.baseOperator, true, this.keyOrIndex);
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
			return new PgCastExpression(this.nest(), "::" + type, (it, ctx) -> TypeCast.getType(type));
		}

		@Override
		public ExpressionTypeContext getType(EvaluationContext context) {
			return asString ? ExpressionTypeContext.of(String.class) : ExpressionTypeContext.object();
		}

		@Override
		public Expression evaluate(EvaluationContext context) {
			return OperatorExpression.create(source.evaluate(context), baseOperator + (asString ? ">>" : ">"),
					context.bind(keyOrIndex));
		}
	}

	static class TypeCast {

		public static QueryExpression.ExpressionTypeContext getType(String type) {

			return switch (type.toLowerCase(Locale.ROOT)) {
				case "varchar", "text" -> QueryExpression.ExpressionTypeContext.string();
				case "boolean" -> QueryExpression.ExpressionTypeContext.bool();
				default -> QueryExpression.ExpressionTypeContext.object();
			};

		}
	}

	record PgCastExpression(QueryExpression source, String typeCast,
			TypeFunction typeFunction) implements PgSql.PostgresQueryExpression {

		static PgCastExpression create(QueryExpression source, String type) {
			return new PgCastExpression(source, "::" + type, (it, context) -> TypeCast.getType(type));
		}

		@Override
		public ExpressionTypeContext getType(EvaluationContext context) {
			return typeFunction.getType(source, context);
		}

		@Override
		public Expression evaluate(EvaluationContext context) {
			return new PostfixExpression(source.evaluate(context), Expressions.just(typeCast));
		}
	}

	record ArrayIndexPostgresExpression(QueryExpression source,
			Object keyOrIndex) implements PgSql.PostgresQueryExpression {

		@Override
		public ExpressionTypeContext getType(EvaluationContext context) {
			return source.getType(context).getActualType();
		}

		@Override
		public Expression evaluate(EvaluationContext context) {
			return new PostfixExpression(source.evaluate(context), new ArrayIndexExpression(context.bind(keyOrIndex)));
		}
	}

}
