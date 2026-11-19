/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import org.jspecify.annotations.Nullable;

import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.data.util.Lazy;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.StringUtils;

/**
 * Internal delegate for Value Expression parsing.
 *
 * @author Mark Paluch
 * @since 4.0.10
 */
class ValueExpressionDelegate {

	private static final ExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();

	private static final ValueExpressionParser PARSER = ValueExpressionParser.create(() -> EXPRESSION_PARSER);

	private static final Lazy<SqlIdentifierExpressionEvaluator> FALLBACK_IDENTIFIER_EVALUATOR = Lazy.of(() -> {

		StandardEnvironment environment = new StandardEnvironment();
		return new SqlIdentifierExpressionEvaluator(it -> {
			return ValueEvaluationContext.of(environment, EvaluationContextProvider.DEFAULT.getEvaluationContext(it));
		});
	});

	/**
	 * Returns a {@link ValueExpression} if the given {@link String} is actually an expression that does not evaluate to a
	 * {@link ValueExpression#isLiteral()} (indicating that no subsequent evaluation is necessary).
	 *
	 * @param potentialExpression can be {@literal null}
	 * @return can be {@literal null}.
	 */
	public static @Nullable ValueExpression detectExpression(@Nullable String potentialExpression) {

		if (!StringUtils.hasText(potentialExpression)) {
			return null;
		}

		ValueExpression expression = PARSER.parse(potentialExpression);
		return expression.isLiteral() ? null : expression;
	}

	/**
	 * Return a default {@link SqlIdentifierExpressionEvaluator}.
	 */
	public static SqlIdentifierExpressionEvaluator getSqlIdentifierExpressionEvaluator() {
		return FALLBACK_IDENTIFIER_EVALUATOR.get();
	}

}
