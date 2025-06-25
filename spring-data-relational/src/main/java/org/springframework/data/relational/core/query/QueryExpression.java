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

import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.sql.BindMarker;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.CheckReturnValue;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * An expression that can be used in a query, such as a column, condition or a function.
 * <p>
 * Expressions represent parts of a query that can be used for selection, ordering, and filtering. It can capture input
 * values and render parameter placeholders for binding values. Reusing an expression reuses the same parameters (values
 * and placeholders).
 *
 * @author Mark Paluch
 * @since 4.0
 */
@FunctionalInterface
public interface QueryExpression {

	/**
	 * Creates a {@link QueryExpression} for a column name (or property path).
	 *
	 * @param name
	 * @return
	 */
	static QueryExpression column(String name) {
		return new CriteriaSources.Column(name);
	}

	/**
	 * Creates a {@link QueryExpression} for a SQL identifier (column or alias name).
	 *
	 * @param identifier
	 * @return
	 */
	static QueryExpression of(SqlIdentifier identifier) {
		return new CriteriaSources.SqlIdentifierSource(identifier);
	}

	/**
	 * Nests this expression by wrapping it in parentheses {@code (…)}. This is useful for grouping expressions in a query
	 * or to clarify an operator scope to avoid ambiguity in complex expressions.
	 *
	 * @return this expression as nested expression.
	 */
	default QueryExpression nest() {
		return new NestedQueryExpression(this);
	}

	/**
	 * Determine the data type used by this expression. Typing is used to determine the target type for conversion and
	 * binding.
	 *
	 * @param context context providing information to evaluate the type of this expression.
	 * @return expression value type.
	 */
	default ExpressionTypeContext getType(EvaluationContext context) {
		return ExpressionTypeContext.object();
	}

	/**
	 * Evaluates this query expression into a SQL {@link Expression} that can be used in a query.
	 * <p>
	 * Evaluation renders an expression from its given input such as a comparison or operator to represent a specific SQL
	 * operation. Implementations can bind values and obtain bind markers during evaluation.
	 *
	 * @param context the context to evaluate this expression in. The context provides access to columns and allows
	 *          binding values.
	 * @return the evaluated SQL expression to be used for SQL rendering.
	 */
	Expression evaluate(EvaluationContext context);

	/**
	 * Interface to bind values to (i.e. during evaluation of a {@link QueryExpression}).
	 */
	interface BindableContext {

		// TODO: we should have a bind method that performs JSON serialization.
		BindMarker bind(Object value);

		BindMarker bind(String name, Object value);
	}

	/**
	 * A mapped column that is referenced by a {@code QueryExpression}. This is typically a column in a table, join, or
	 * subselect.
	 */
	interface MappedColumn extends ExpressionTypeContext {

		/**
		 * @return the {@link Expression} representing this column.
		 */
		Expression toExpression();

	}

	/**
	 * {@link QueryExpression}s are evaluated within this context and can obtain details about columns and bind values to
	 * it.
	 */
	interface EvaluationContext extends BindableContext {

		/**
		 * Retrieve a mapped column by its identifier along with typing information. If the identifier does not map to a
		 * property path, returns the plain column name without detailed type information.
		 *
		 * @param identifier name of the column.
		 * @return
		 */
		MappedColumn getColumn(SqlIdentifier identifier);

		/**
		 * Retrieve a mapped column by its identifier along with typing information. If the identifier does not map to a
		 * property path, returns the plain column name without detailed type information.
		 *
		 * @param column column name. Can contain a property path.
		 * @return
		 */
		MappedColumn getColumn(String column);

		/**
		 * Creates a new {@code EvaluationContext} typed to the {@link ExpressionTypeContext}.
		 *
		 * @param type the type to use for this context. This is used to determine the target type for conversion and
		 *          binding.
		 * @return a new typed {@code EvaluationContext}.
		 */
		@CheckReturnValue
		EvaluationContext withType(ExpressionTypeContext type);

		/**
		 * Creates a new {@code EvaluationContext} typed to the {@link QueryExpression}.
		 *
		 * @param expression the expression to derive its type from to use for this context. This is used to determine the
		 *          target type for conversion and binding.
		 * @return a new typed {@code EvaluationContext}.
		 */
		default EvaluationContext withType(QueryExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return withType(expression.getType(this));
		}
	}

	/**
	 * Interface providing details about the value type used by an expression.
	 * <p>
	 * Provides factory methods to create {@link ExpressionTypeContext} instances and methods to introspect the type.
	 * TODO: Better name.
	 */
	interface ExpressionTypeContext {

		static ExpressionTypeContext object() {
			return SimpleTypeContext.OBJECT;
		}

		static ExpressionTypeContext bool() {
			return SimpleTypeContext.BOOL;
		}

		static ExpressionTypeContext of(Class<?> type) {
			return new SimpleTypeContext(TypeInformation.of(type));
		}

		static ExpressionTypeContext string() {
			return SimpleTypeContext.STRING;
		}

		TypeInformation<?> getTargetType();

		@Nullable
		RelationalPersistentProperty getProperty();

		/**
		 * Nest the type as collection.
		 *
		 * @return
		 */
		default ExpressionTypeContext asCollection() {
			return new SimpleTypeContext(TypeInformation.of(getTargetType().toResolvableType().asCollection()));
		}

		/**
		 * Returns the collection component type (collection or map).
		 *
		 * @return
		 */
		default ExpressionTypeContext getActualType() {
			return new SimpleTypeContext(getTargetType().getRequiredActualType());
		}

		/**
		 * Returns the assignable type that is shared by this and the {@code other} type.
		 *
		 * @param other the other type to compare with.
		 * @return the assignable type that is shared by this and the {@code other} type.
		 */
		default ExpressionTypeContext getAssignableType(ExpressionTypeContext other) {

			// narrow check: cross-assignability
			if (getTargetType().isAssignableFrom(other.getTargetType())
					&& other.getTargetType().isAssignableFrom(getTargetType())) {
				return this;
			}

			// other might be more specific
			if (getTargetType().isAssignableFrom(other.getTargetType())) {
				return other;
			}

			// this might be more specific
			if (other.getTargetType().isAssignableFrom(getTargetType())) {
				return this;
			}

			return ExpressionTypeContext.object();
		}
	}

	/**
	 * Interface to determine its expression value type based on the expression and the render context.
	 */
	@FunctionalInterface
	interface TypeFunction {

		ExpressionTypeContext getType(QueryExpression expression, EvaluationContext context);
	}

}
