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

import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Utility class providing implementations of {@link CriteriaSource} for common sources such as columns and SQL
 * identifiers.
 *
 * @author Mark Paluch
 * @since 4.0
 */
class CriteriaSources {

	/**
	 * A column name (or property path) reference.
	 *
	 * @param name
	 */
	public record Column(String name) implements QueryExpression {

		@Override
		public ExpressionTypeContext getType(EvaluationContext context) {
			return context.getColumn(name);
		}

		@Override
		public String getNameHint() {
			return name;
		}

		@Override
		public Expression evaluate(EvaluationContext context) {
			return context.getColumn(name).toExpression();
		}

		@Override
		public String toString() {
			return name;
		}
	}

	/**
	 * A column or alias name.
	 *
	 * @param identifier
	 */
	public record SqlIdentifierSource(SqlIdentifier identifier) implements QueryExpression {

		@Override
		public ExpressionTypeContext getType(EvaluationContext context) {
			return context.getColumn(identifier);
		}

		@Override
		public String getNameHint() {
			return identifier.getReference();
		}

		@Override
		public Expression evaluate(EvaluationContext context) {
			return context.getColumn(identifier).toExpression();
		}

		@Override
		public String toString() {
			return identifier.toSql(IdentifierProcessing.NONE);
		}

	}

}
