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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.Vector;
import org.springframework.data.relational.core.query.PgSqlImpl.ValuesExpression;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Dialect-specific extension for the Postgres database dialect.
 *
 * @author Mark Paluch
 * @since 4.0
 */
public final class PgSql {

	private PgSql() {

	}

	/**
	 * Creates a function expression for the given {@code functionName} using {@code arguments}. The function name is
	 * passed on directly to the SQL text so it must be a valid function name. Make sure to sanitize the function name
	 * when accepting it from user input.
	 *
	 * @param functionName name of the function to call, must not be {@literal null} or empty.
	 * @param arguments function arguments, can be any {@link QueryExpression} or simple values like {@code String},
	 *          {@code Integer}.
	 * @return the function expression for {@code functionName} with the given {@code arguments}.
	 */
	public static PostgresQueryExpression function(String functionName, Object... arguments) {
		return new PgSqlImpl.FunctionExpression(functionName, Arrays.asList(arguments));
	}

	/**
	 * Entry point for creating a {@link PgCriteria.PostgresCriteriaStep} for the given column (or property specified as
	 * property path).
	 *
	 * @param column
	 * @return
	 */
	public static PgCriteria.PostgresCriteriaStep where(String column) {
		return PgCriteria.where(column);
	}

	/**
	 * Entry point for creating a {@link PgCriteria.PostgresCriteriaStep} for the given column (or property specified as
	 * property path) that shall be wrapped by a wrapping function to process the column contents before its use in a
	 * downstream condition or expression.
	 *
	 * @param column
	 * @param wrappingFunction
	 * @return
	 */
	public static PgCriteria.PostgresCriteriaStep where(String column,
			Function<Operators, QueryExpression> wrappingFunction) {
		return PgCriteria.where(column, wrappingFunction);
	}

	/**
	 * Entry point for creating a {@link PgCriteria.PostgresCriteriaStep} for the given {@link QueryExpression
	 * expression}.
	 *
	 * @param expression
	 * @return
	 */
	public static PgCriteria.PostgresCriteriaStep where(QueryExpression expression) {
		return PgCriteria.where(expression);
	}

	/**
	 * Entry point for creating a {@link PgCriteria.PostgresCriteriaStep} for the given {@link QueryExpression} that shall
	 * be wrapped by a wrapping function to process the column contents before its use in a downstream condition or
	 * expression.
	 *
	 * @param expression
	 * @param wrappingFunction
	 * @return
	 */
	public static PgCriteria.PostgresCriteriaStep where(QueryExpression expression,
			Function<Operators, QueryExpression> wrappingFunction) {
		return PgCriteria.where(expression, wrappingFunction);
	}

	/**
	 * Entrypoint for array functions.
	 *
	 * @return
	 */
	public static ArrayFunctions arrays() {

		return new ArrayFunctions() {
			@Override
			public QueryExpression arrayOf(Object... values) {
				return new PgSqlImpl.ArrayExpression(Arrays.asList(values));
			}
		};
	}

	/**
	 * Entrypoint for JSON functions.
	 *
	 * @return
	 */
	public static JsonFunctions json() {

		return new JsonFunctions() {

			@Override
			public QueryExpression jsonOf(Map<String, Object> jsonObject) {
				return new PgSqlImpl.JsonExpression(jsonObject, "json");
			}

			@Override
			public QueryExpression jsonbOf(Map<String, Object> jsonObject) {
				return new PgSqlImpl.JsonExpression(jsonObject, "jsonb");
			}
		};
	}

	public static VectorSearchFunctions vectorSearch() {
		return new VectorSearchFunctions() {
			@Override
			public VectorSearchDistanceStep distanceOf(String column, Vector vector) {
				return new VectorSearchDistanceStep() {
					@Override
					public PostgresQueryExpression using(ScoringFunction scoringFunction) {
						return new PgSqlImpl.DistanceFunction(QueryExpression.column(column), scoringFunction, vector);
					}

				};
			}
		};
	}

	/**
	 * Postgres-specific {@link Criteria} implementation that provides access to Postgres-specific operators and
	 * functions.
	 */
	public static class PgCriteria extends Criteria {

		protected PgCriteria(QueryExpression source) {
			super(source);
		}

		protected PgCriteria(@Nullable Criteria previous, Combinator combinator, List<CriteriaDefinition> group,
				@Nullable QueryExpression queryExpression, @Nullable Comparator comparator, @Nullable Object value) {
			super(previous, combinator, group, queryExpression, null, comparator, value);
		}

		protected PgCriteria(@Nullable Criteria previous, Combinator combinator, List<CriteriaDefinition> group,
				@Nullable QueryExpression queryExpression, @Nullable Comparator comparator, @Nullable Object value,
				boolean ignoreCase) {
			super(previous, combinator, group, queryExpression, null, comparator, value, ignoreCase);
		}

		/**
		 * Entry point for creating a {@link PgCriteria.PostgresCriteriaStep} for the given column (or property specified as
		 * property path).
		 *
		 * @param column
		 * @return
		 */
		public static PgCriteria.PostgresCriteriaStep where(String column) {
			return new PgSqlImpl.DefaultPostgresCriteriaStep(column);
		}

		/**
		 * Entry point for creating a {@link PgCriteria.PostgresCriteriaStep} for the given column (or property specified as
		 * property path) that shall be wrapped by a wrapping function to process the column contents before its use in a
		 * downstream condition or expression.
		 *
		 * @param column
		 * @param wrappingFunction
		 * @return
		 */
		public static PgCriteria.PostgresCriteriaStep where(String column,
				Function<Operators, QueryExpression> wrappingFunction) {

			return where(new CriteriaSources.Column(column), wrappingFunction);
		}

		/**
		 * Entry point for creating a {@link PgCriteria.PostgresCriteriaStep} for the given {@link QueryExpression
		 * expression}.
		 *
		 * @param expression
		 * @return
		 */
		public static PgCriteria.PostgresCriteriaStep where(QueryExpression expression) {
			return new PgSqlImpl.DefaultPostgresCriteriaStep(expression);
		}

		/**
		 * Entry point for creating a {@link PgCriteria.PostgresCriteriaStep} for the given {@link QueryExpression} that
		 * shall be wrapped by a wrapping function to process the column contents before its use in a downstream condition
		 * or expression.
		 *
		 * @param expression
		 * @param wrappingFunction
		 * @return
		 */
		public static PgCriteria.PostgresCriteriaStep where(QueryExpression expression,
				Function<Operators, QueryExpression> wrappingFunction) {

			PgSqlImpl.DefaultOperators functions = new PgSqlImpl.DefaultOperators(expression);
			return where(wrappingFunction.apply(functions));
		}

		@Override
		public PgCriteria.PostgresCriteriaStep and(String column) {

			Assert.hasText(column, "Column name must not be null or empty");

			return and(QueryExpression.column(column));
		}

		public PgCriteria.PostgresCriteriaStep and(String column, Function<Operators, QueryExpression> wrappingFunction) {

			Assert.hasText(column, "Column name must not be null or empty");

			return and(wrappingFunction.apply(new PgSqlImpl.DefaultOperators(QueryExpression.column(column))));
		}

		@Override
		public PgCriteria.PostgresCriteriaStep and(QueryExpression expression) {

			Assert.notNull(expression, "Query expression must not be null");

			return createStep(expression, Combinator.AND);
		}

		@Override
		public PgCriteria.PostgresCriteriaStep or(String column) {

			Assert.hasText(column, "Column name must not be null or empty");

			return or(QueryExpression.column(column));
		}

		public PgCriteria.PostgresCriteriaStep or(String column, Function<Operators, QueryExpression> wrappingFunction) {

			Assert.hasText(column, "Column name must not be null or empty");

			return or(wrappingFunction.apply(new PgSqlImpl.DefaultOperators(QueryExpression.column(column))));
		}

		@Override
		public PgCriteria.PostgresCriteriaStep or(QueryExpression expression) {

			Assert.notNull(expression, "Query expression must not be null");

			return createStep(expression, Combinator.OR);
		}

		private PgSqlImpl.DefaultPostgresCriteriaStep createStep(QueryExpression expression, Combinator combinator) {

			return new PgSqlImpl.DefaultPostgresCriteriaStep(expression) {

				@Override
				protected PgCriteria createCriteria(Comparator comparator, @Nullable Object value) {
					return new PgCriteria(PgCriteria.this, combinator, Collections.emptyList(), expression, comparator, value);
				}

				@Override
				protected PgCriteria createCriteria(QueryExpression queryExpression) {
					return new PgCriteria(PgCriteria.this, combinator, Collections.emptyList(), expression, null, null);
				}
			};
		}

		/**
		 * Interface providing a fluent API builder methods to build a {@link Criteria}.
		 */
		public interface PostgresCriteriaStep extends CriteriaStep {

			/**
			 * Array criteria steps.
			 *
			 * @return
			 */
			PostgresArrayCriteriaStep arrays();

			/**
			 * JSON criteria steps.
			 *
			 * @return
			 */
			PgCriteria json(Function<PostgresJsonQueryExpression, QueryExpression> criteriaFunction);

			/**
			 * Consider the previous expression as expression evaluating a boolean result.
			 *
			 * @return
			 */
			PgCriteria asBoolean();

			@Override
			PgCriteria is(Object value);

			@Override
			PgCriteria not(Object value);

			@Override
			PgCriteria in(Object... values);

			@Override
			PgCriteria in(Collection<?> values);

			@Override
			PgCriteria notIn(Object... values);

			@Override
			PgCriteria notIn(Collection<?> values);

			@Override
			PgCriteria between(Object begin, Object end);

			@Override
			PgCriteria notBetween(Object begin, Object end);

			@Override
			PgCriteria lessThan(Object value);

			@Override
			PgCriteria lessThanOrEquals(Object value);

			@Override
			PgCriteria greaterThan(Object value);

			@Override
			PgCriteria greaterThanOrEquals(Object value);

			@Override
			PgCriteria like(Object value);

			@Override
			PgCriteria notLike(Object value);

			@Override
			PgCriteria isNull();

			@Override
			PgCriteria isNotNull();

			@Override
			PgCriteria isTrue();

			@Override
			PgCriteria isFalse();
		}

		/**
		 * Fluent Postgres-specific Array criteria API providing access to Array operators and functions.
		 */
		public interface PostgresArrayCriteriaStep {

			/**
			 * Does the first array contain the second, that is, does each element appearing in the second array equal some
			 * element of the first array using {@code @>}.
			 *
			 * @param expression
			 * @return
			 */
			PgCriteria contains(QueryExpression expression);

			/**
			 * Does the first array contain the second, that is, does each element appearing in the second array equal some
			 * element of the first array using {@code @>}.
			 *
			 * @param column
			 * @property
			 */
			default PgCriteria contains(String column) {
				return contains(QueryExpression.column(column));
			}

			/**
			 * Does the first array contain {@code values}, that is, does each element appearing in the second array equal
			 * some element of the first array using {@code @>}.
			 *
			 * @param values
			 * @property
			 */
			default PgCriteria contains(Object... values) {
				return contains(PgSqlImpl.ArrayExpression.expressionOrWrap(values));
			}

			/**
			 * Do the arrays overlap, that is, have any elements in common using {@code &&}.
			 *
			 * @param expression
			 * @return
			 */
			PgCriteria overlaps(QueryExpression expression);

			/**
			 * Do the arrays overlap, that is, have any elements in common using {@code &&}.
			 *
			 * @param column
			 * @return
			 */
			default PgCriteria overlaps(String column) {
				return overlaps(QueryExpression.column(column));
			}

			/**
			 * Do the arrays overlap, that is, have any elements in common using {@code &&}.
			 *
			 * @param values
			 * @property
			 */
			default PgCriteria overlaps(Object... values) {
				return overlaps(PgSqlImpl.ArrayExpression.expressionOrWrap(values));
			}

		}

		/**
		 * Fluent Postgres-specific JSON criteria API providing access to JSON operators and functions.
		 */
		public interface PostgresJsonCriteriaStep {

			/**
			 * {@code ?} operator for JSON exists.
			 *
			 * @param column
			 * @return
			 */
			default PgCriteria exists(String column) {
				return exists(QueryExpression.column(column));
			}

			/**
			 * {@code ?} operator for JSON exists.
			 *
			 * @param expression
			 * @return
			 */
			PgCriteria exists(QueryExpression expression);

			/**
			 * {@code ?} operator for JSON exists.
			 *
			 * @param value
			 * @return
			 */
			default PgCriteria exists(Object value) {
				return exists(new PgSqlImpl.ValueExpression(value));
			}

			/**
			 * {@code ?|} operator for JSON contains.
			 *
			 * @param value
			 * @return
			 */
			default PgCriteria contains(String field) {
				return contains(QueryExpression.column(field));
			}

			/**
			 * {@code ?|} operator for JSON contains.
			 *
			 * @param value
			 * @return
			 */
			PgCriteria contains(Object value);

			/**
			 * {@code ?&} operator for JSON contains all.
			 *
			 * @param values
			 * @return
			 */
			default PgCriteria containsAll(Object... values) {
				return containsAll(Arrays.asList(values));
			}

			/**
			 * {@code ?&} operator for JSON contains all.
			 *
			 * @param values
			 * @return
			 */
			PgCriteria containsAll(Collection<Object> values);

			/**
			 * {@code ?|} operator for JSON contains any.
			 *
			 * @param values
			 * @return
			 */
			default PgCriteria containsAny(Object... values) {
				return containsAny(Arrays.asList(values));
			}

			/**
			 * {@code ?|} operator for JSON contains any.
			 *
			 * @param values
			 * @return
			 */
			PgCriteria containsAny(Collection<Object> values);

			/**
			 * Does JSON path return any item for the specified JSON value using the {@code @?} operator.
			 *
			 * @param jsonPath
			 * @return
			 */
			PgCriteria jsonPathMatches(String jsonPath);

			/**
			 * Returns the result of a JSON path predicate check for the specified JSON value using the {@code @@} operator.
			 *
			 * @param jsonPath
			 * @return
			 */
			PgCriteria jsonPath(String jsonPath);

		}
	}

	/**
	 * Postgres-specific Array functions for querying array data.
	 */
	public interface ArrayFunctions {

		/**
		 * Constructs an {@code ARRAY[…]} expression from the given values.
		 *
		 * @param values
		 * @return an expression that represents a Postgres array of the given values.
		 */
		QueryExpression arrayOf(Object... values);

	}

	/**
	 * Postgres-specific JSON functions for querying JSON data.
	 */
	public interface JsonFunctions {

		/**
		 * Constructs an {@code JSON} value from the given {@code jsonObject}.
		 *
		 * @param jsonObject
		 * @return a JSONB value that represents the given JSON object.
		 */
		QueryExpression jsonOf(Map<String, Object> jsonObject);

		/**
		 * Constructs an {@code JSONB} value from the given {@code jsonObject}.
		 *
		 * @param jsonObject
		 * @return a JSONB value that represents the given JSON object.
		 */
		QueryExpression jsonbOf(Map<String, Object> jsonObject);

	}

	/**
	 * Postgres-specific Vector Search functions.
	 */
	public interface VectorSearchFunctions {

		/**
		 * Start building a Vector Search operator given a column and a reference {@link Vector}
		 *
		 * @param column
		 * @param vector
		 * @return
		 */
		VectorSearchDistanceStep distanceOf(String column, Vector vector);

		interface VectorSearchDistanceStep {

			default PostgresQueryExpression l2() {
				return using(ScoringFunction.euclidean());
			}

			default PostgresQueryExpression innerProduct() {
				return using(ScoringFunction.dotProduct());
			}

			default PostgresQueryExpression cosine() {
				return using(ScoringFunction.cosine());
			}

			default PostgresQueryExpression l1() {
				return using(of("<+>"));
			}

			default PostgresQueryExpression hamming() {
				return using(of("<~>"));
			}

			default PostgresQueryExpression jaccard() {
				return using(of("<%>"));
			}

			default ScoringFunction of(String operator) {
				return () -> operator;
			}

			PostgresQueryExpression using(ScoringFunction scoringFunction);

		}
	}

	/**
	 * Entrypoint for Postgres-specific functions.
	 */
	public interface Operators extends ArrayOperators, JsonOperators, VectorSearchOperators {

		/**
		 * Returns a {@link ArrayOperators} object providing access to array functions.
		 */
		default ArrayOperators array() {
			return this;
		}

		/**
		 * Returns a {@link JsonOperators} object providing access to JSON functions.
		 */
		default JsonOperators json() {
			return this;
		}

		/**
		 * Returns a {@link VectorSearchOperators} object providing access to pgvector functions.
		 */
		default VectorSearchOperators vector() {
			return this;
		}

	}

	/**
	 * pgvector-specific functions for Postgres Vector Search.
	 */
	public interface VectorSearchOperators {

		/**
		 * Calculates the distance to the given vector using the specified distance function.
		 *
		 * @param vector the vector to calculate the distance to.
		 * @param distanceFunction distance function to use, e.g. {@link Distances#l2()},
		 * @return an expression representing the distance calculation.
		 */
		PostgresQueryExpression distanceTo(Vector vector, Function<Distances, ScoringFunction> distanceFunction);

		/**
		 * Calculates the distance to the given vector using the specified distance function.
		 *
		 * @param vector the vector to calculate the distance to.
		 * @param scoringFunction scoring function to use, e.g. {@link Distances#l2()},
		 * @return an expression representing the distance calculation.
		 */
		default PostgresQueryExpression distanceTo(Vector vector, ScoringFunction scoringFunction) {
			return distanceTo(vector, distances -> scoringFunction);
		}

		interface Distances {

			default ScoringFunction l2() {
				return ScoringFunction.euclidean();
			}

			default ScoringFunction innerProduct() {
				return ScoringFunction.dotProduct();
			}

			default ScoringFunction cosine() {
				return ScoringFunction.cosine();
			}

			default ScoringFunction l1() {
				return of("<+>");
			}

			default ScoringFunction hamming() {
				return of("<~>");
			}

			default ScoringFunction jaccard() {
				return of("<%>");
			}

			default ScoringFunction of(String operator) {
				return () -> operator;
			}
		}
	}

	/**
	 * Postgres-specific JSON functions for querying JSON data.
	 */
	public interface JsonOperators {

		/**
		 * Creates an index expression to extract a value from a JSON array at the given {@code index} using the arrow
		 * {@code ->} operator.
		 *
		 * @param index index of the element to extract from the JSON array.
		 * @return an expression that extracts the value at the specified {@code index}.
		 */
		PostgresQueryExpression index(int index);

		/**
		 * Creates an index expression to extract a value from a JSON object at the given {@code field} using the arrow
		 * {@code ->} operator.
		 *
		 * @param field name of the field to extract from the JSON object.
		 * @return an expression that extracts the value using the specified {@code field}.
		 */
		PostgresQueryExpression field(String field);
	}

	/**
	 * Postgres-specific Array operators.
	 */
	public interface ArrayOperators {

		/**
		 * Does the first array contain the second, that is, does each element appearing in the second array equal some
		 * element of the first array using {@code @>}.
		 *
		 * @param expression
		 * @return
		 */
		PostgresQueryExpression contains(QueryExpression expression);

		/**
		 * Does the first array contain the second, that is, does each element appearing in the second array equal some
		 * element of the first array using {@code @>}.
		 *
		 * @param column
		 * @property
		 */
		default PostgresQueryExpression contains(String column) {
			return contains(QueryExpression.column(column));
		}

		/**
		 * Does the first array contain {@code values}, that is, does each element appearing in the second array equal some
		 * element of the first array using {@code @>}.
		 *
		 * @param values
		 * @property
		 */
		default PostgresQueryExpression contains(Object... values) {
			return contains(ValuesExpression.oneOrMany(values));
		}

		/**
		 * Do the arrays overlap, that is, have any elements in common using {@code &&}.
		 *
		 * @param expression
		 * @return
		 */
		PostgresQueryExpression overlaps(QueryExpression expression);

		/**
		 * Do the arrays overlap, that is, have any elements in common using {@code &&}.
		 *
		 * @param column
		 * @return
		 */
		default PostgresQueryExpression overlaps(String column) {
			return overlaps(QueryExpression.column(column));
		}

		/**
		 * Do the arrays overlap, that is, have any elements in common using {@code &&}.
		 *
		 * @param values
		 * @property
		 */
		default PostgresQueryExpression overlaps(Object... values) {
			return overlaps(ValuesExpression.oneOrMany(values));
		}

		/**
		 * Concatenates this the current array with the given {@code expression} representing the array concatenation
		 * operator.
		 *
		 * @param expression
		 * @return
		 */
		PostgresQueryExpression concatWith(QueryExpression expression);

		/**
		 * Concatenates this the current array with the given {@code property} representing the array concatenation
		 * operator.
		 *
		 * @param column
		 * @return
		 */
		default PostgresQueryExpression concatWith(String column) {
			return concatWith(QueryExpression.column(column));
		}

		/**
		 * Concatenates this the current array with the given {@code values} representing the array concatenation operator.
		 *
		 * @param values
		 * @property
		 */
		default PostgresQueryExpression concatWith(Object... values) {
			return concatWith(ValuesExpression.oneOrMany(values));
		}

	}

	/**
	 * Postgres-specific query expression that allows to append Postgres-specific type casts and array indexing.
	 */
	public interface PostgresQueryExpression extends QueryExpression {

		/**
		 * Cast the expression to the given type by appending {@code ::type} to the expression.
		 *
		 * @param type
		 * @return
		 */
		default PostgresQueryExpression as(String type) {
			return PgSqlImpl.PgCastExpression.create(this, type);
		}

		/**
		 * Cast the expression to boolean type by appending {@code ::boolean} to the expression.
		 *
		 * @return the casted expression.
		 */
		default PostgresQueryExpression asBoolean() {
			return as("boolean");
		}

		/**
		 * Cast the expression to varchar type by appending {@code ::varchar} to the expression.
		 *
		 * @return the casted expression.
		 */
		default PostgresQueryExpression asString() {
			return as("varchar");
		}

		/**
		 * Cast the expression to varchar type by appending {@code ::json} the expression.
		 *
		 * @return the casted expression.
		 */
		default PostgresJsonQueryExpression asJson() {
			return (PostgresJsonQueryExpression) as("json");
		}

		/**
		 * Cast the expression to varchar type by appending {@code ::jsonb} the expression.
		 *
		 * @return the casted expression.
		 */
		default PostgresJsonQueryExpression asJsonb() {
			return (PostgresJsonQueryExpression) as("jsonb");
		}

		default PostgresQueryExpression json(Function<PostgresJsonQueryExpression, PostgresQueryExpression> jsonFunction) {
			return jsonFunction.apply((PostgresJsonQueryExpression) this);
		}

		/**
		 * Creates an index expression for the given index using {@code […]} brackets. Can be used with arrays or JSON
		 * arrays.
		 *
		 * @param index
		 * @return
		 */
		default PostgresQueryExpression index(int index) {
			return new PgSqlImpl.ArrayIndexPostgresExpression(this, index);
		}

		/**
		 * Creates an index expression for the given index using {@code […]} brackets. Can be used with JSON arrays for
		 * by-field access.
		 *
		 * @param key
		 * @return
		 */
		default PostgresQueryExpression element(String key) {
			return new PgSqlImpl.ArrayIndexPostgresExpression(this, key);
		}

	}

	/**
	 * JSON-specific query expression that allows to append Postgres-specific JSON operators and functions.
	 */
	public interface PostgresJsonQueryExpression {

		/**
		 * {@code ?} operator for JSON exists.
		 *
		 * @param expression
		 * @return
		 */
		PostgresQueryExpression exists(QueryExpression expression);

		/**
		 * {@code ?} operator for JSON exists.
		 *
		 * @param value
		 * @return
		 */
		default PostgresQueryExpression exists(Object value) {
			return exists(new PgSqlImpl.ValueExpression(value));
		}

		/**
		 * {@code ?|} operator for JSON contains.
		 *
		 * @param value
		 * @return
		 */
		PostgresQueryExpression contains(Object value);

		/**
		 * {@code ?&} operator for JSON contains all.
		 *
		 * @param values
		 * @return
		 */
		default PostgresQueryExpression containsAll(Object... values) {
			return containsAll(Arrays.asList(values));
		}

		/**
		 * {@code ?&} operator for JSON contains all.
		 *
		 * @param values
		 * @return
		 */
		PostgresQueryExpression containsAll(Collection<Object> values);

		/**
		 * {@code ?|} operator for JSON contains any.
		 *
		 * @param values
		 * @return
		 */
		default PostgresQueryExpression containsAny(Object... values) {
			return containsAny(Arrays.asList(values));
		}

		/**
		 * {@code ?|} operator for JSON contains any.
		 *
		 * @param values
		 * @return
		 */
		PostgresQueryExpression containsAny(Collection<Object> values);

		/**
		 * Does JSON path return any item for the specified JSON value using the {@code @?} operator.
		 *
		 * @param jsonPath
		 * @return
		 */
		PostgresQueryExpression jsonPathMatches(String jsonPath);

		/**
		 * Returns the result of a JSON path predicate check for the specified JSON value using the {@code @@} operator.
		 *
		 * @param jsonPath
		 * @return
		 */
		PostgresQueryExpression jsonPath(String jsonPath);

	}

}
