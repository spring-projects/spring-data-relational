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
import java.util.function.Function;

import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.Vector;

/**
 * @author Mark Paluch
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
			Function<Functions, QueryExpression> wrappingFunction) {

		PgSqlImpl.DefaultFunctions functions = new PgSqlImpl.DefaultFunctions(new CriteriaSources.DotPath(column));
		return where(wrappingFunction.apply(functions));
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

	public static ArrayFunctions arrays() {

		return new ArrayFunctions() {
			@Override
			public QueryExpression arrayOf(Object... values) {
				return new PgSqlImpl.ArrayExpression(Arrays.asList(values));
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

		public interface PostgresCriteriaStep extends CriteriaStep {

			/**
			 * {@code ?} operator for JSONB containment.
			 *
			 * @param value
			 * @return
			 */
			PgCriteria exists(Object value);

			/**
			 * {@code @>} operator for JSONB containment.
			 *
			 * @param value
			 * @return
			 */
			PgCriteria contains(Object value);

			/**
			 * {@code &&} operator for array containment.
			 *
			 * @param value
			 * @return
			 */
			PgCriteria overlaps(Object value);

			PostgresJsonCriteriaStep json();

		}

		/**
		 * Fluent Postgres-specific JSON criteria API providing access to JSON operators and functions.
		 */
		public interface PostgresJsonCriteriaStep {

			PgCriteria contains(String field);

			PgCriteria contains(Object value);

			PgCriteria containsAll(Iterable<Object> values);

			PgCriteria containsAll(Object... values);

			PgCriteria containsAny(Iterable<Object> values);

			PgCriteria containsAny(Object... values);

			// @?
			PgCriteria jsonPathMatches(String jsonPath);

			// @@
			PgCriteria jsonPath(String jsonPath);

		}
	}

	/**
	 * Entrypoint for Postgres-specific functions.
	 */
	public interface Functions extends VectorSearchFunctions, JsonFunctions {

		/**
		 * Returns a {@link JsonFunctions} object providing access to JSON functions.
		 */
		default JsonFunctions json() {
			return this;
		}

		/**
		 * Returns a {@link VectorSearchFunctions} object providing access to pgvector functions.
		 */
		default VectorSearchFunctions vector() {
			return this;
		}

	}

	/**
	 * pgvector-specific functions for Postgres Vector Search.
	 */
	public interface VectorSearchFunctions {

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
			return new PgSqlImpl.AppendingPostgresExpression(this, "::" + type);
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
		default PostgresQueryExpression asJson() {
			return as("json");
		}

		/**
		 * Cast the expression to varchar type by appending {@code ::jsonb} the expression.
		 *
		 * @return the casted expression.
		 */
		default PostgresQueryExpression asJsonb() {
			return as("jsonb");
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

}
