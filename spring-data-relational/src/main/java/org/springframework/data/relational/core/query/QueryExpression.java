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

import org.springframework.data.relational.core.sql.BindMarker;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * An expression that can be used in a query, such as a column, condition or a function.
 * <p>
 * Expressions represent parts of a query that can be used for selection, ordering, and filtering. It can capture input
 * values and render parameter placeholders for binding values. Reusing an expression reuses the same parameters (values
 * and placeholders).
 *
 * @author Mark Paluch
 * @since 4.0s
 */
public interface QueryExpression {

	/**
	 * Nests this expression by wrapping it in parentheses {@code (…)}. This is useful for grouping expressions in a query
	 * or to clarify an operator scope to avoid ambiguity in complex expressions.
	 *
	 * @return this expression as nested expression.
	 */
	default QueryExpression nest() {
		return new NestedQueryExpression(this);
	}

	// TODO: Used to determine the inner-most context.
	default QueryRenderContext contextualize(QueryRenderContext context) {
		return context;
	}

	/**
	 * Renders this expression into a SQL {@link Expression} that can be used in a query.
	 *
	 * @implNote an expression such as a comparison requires a contextualized {@link QueryRenderContext} that considers
	 *           the left-hand side and the right-hand side of the expression. It is necessary to determine the target
	 *           type when comparing a column to a given value. But what if we compare two columns? Something like:
	 *
	 *           <pre class="code">
	 *           CREATE TABLE person (id int, name varchar(100), country varchar(2), no_visa_required varchar[2]);
	 *           SELECT * FROM person WHERE no_visa_required && (overlap, any elements in common) ARRAY('DE, country, '??');
	 *           SELECT * FROM person WHERE (no_visa_required || country) && (overlap) ARRAY('DE, '??');
	 *           </pre>
	 *
	 * @param context
	 * @return
	 */
	Expression render(QueryRenderContext context);

	// TODO better design to support nested expressions
	interface QueryRenderContext {

		QueryRenderContext withProperty(String dotPath);

		QueryRenderContext withProperty(SqlIdentifier identifier);

		Expression getColumnName(SqlIdentifier identifier);

		Expression getColumnName(String dotPath);

		// only available after contextualization
		Expression getColumnName();

		BindMarker bind(Object value);

		BindMarker bind(String name, Object value);

		Object writeValue(Object value);

	}

}
