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
import org.springframework.data.relational.core.sql.Expressions;

/**
 * @author Mark Paluch
 */
class NestedQueryExpression implements QueryExpression {

	private final QueryExpression expression;

	public NestedQueryExpression(QueryExpression expression) {
		this.expression = expression;
	}

	@Override
	public QueryExpression nest() {
		return this;
	}

	@Override
	public QueryRenderContext contextualize(QueryRenderContext context) {
		return expression.contextualize(context);
	}

	@Override
	public Expression render(QueryRenderContext context) {
		return Expressions.nest(expression.render(context));
	}

}
