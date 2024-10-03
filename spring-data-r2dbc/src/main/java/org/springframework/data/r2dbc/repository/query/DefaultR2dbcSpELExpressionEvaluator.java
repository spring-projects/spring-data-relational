/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.r2dbc.repository.query;

import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.mapping.model.SpELExpressionEvaluator;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.r2dbc.core.Parameter;

/**
 * Simple {@link R2dbcSpELExpressionEvaluator} implementation using {@link ExpressionParser} and
 * {@link EvaluationContext}.
 *
 * @author Mark Paluch
 * @since 1.2
 */
class DefaultR2dbcSpELExpressionEvaluator implements R2dbcSpELExpressionEvaluator {

	private final ValueExpressionDelegate delegate;

	private final ValueEvaluationContext context;

	DefaultR2dbcSpELExpressionEvaluator(ValueExpressionDelegate delegate, ValueEvaluationContext context) {
		this.delegate = delegate;
		this.context = context;
	}

	/**
	 * Return a {@link SpELExpressionEvaluator} that does not support expression evaluation.
	 *
	 * @return a {@link SpELExpressionEvaluator} that does not support expression evaluation.
	 */
	public static R2dbcSpELExpressionEvaluator unsupported() {
		return NoOpExpressionEvaluator.INSTANCE;
	}

	@Override
	public Parameter evaluate(String expression) {

		ValueExpression expr = delegate.parse(expression);

		Object value = expr.evaluate(context);
		Class<?> valueType = value != null ? value.getClass() : Object.class;

		return org.springframework.r2dbc.core.Parameter.fromOrEmpty(value, valueType);
	}

	/**
	 * {@link SpELExpressionEvaluator} that does not support SpEL evaluation.
	 *
	 * @author Mark Paluch
	 */
	enum NoOpExpressionEvaluator implements R2dbcSpELExpressionEvaluator {

		INSTANCE;

		@Override
		public Parameter evaluate(String expression) {
			throw new UnsupportedOperationException("Expression evaluation not supported");
		}
	}
}
