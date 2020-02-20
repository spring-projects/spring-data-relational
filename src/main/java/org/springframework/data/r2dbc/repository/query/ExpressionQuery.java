/*
 * Copyright 2020 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.lang.Nullable;

/**
 * Query using Spring Expression Language to indicate parameter bindings. Queries using SpEL use {@code :#{…}} to
 * enclose expressions. Expressions are substituted with synthetic named parameters and require therefore enabled named
 * parameter expansion for execution.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class ExpressionQuery {

	private static final char CURLY_BRACE_OPEN = '{';
	private static final char CURLY_BRACE_CLOSE = '}';

	private static final String SYNTHETIC_PARAMETER_TEMPLATE = "__synthetic_%d__";

	private static final Pattern EXPRESSION_BINDING_PATTERN = Pattern.compile("[:]#\\{(.*)}");

	private final String query;

	private final List<ParameterBinding> parameterBindings;

	private ExpressionQuery(String query, List<ParameterBinding> parameterBindings) {
		this.query = query;
		this.parameterBindings = parameterBindings;
	}

	/**
	 * Create a {@link ExpressionQuery} from a {@code query}.
	 *
	 * @param query the query string to parse.
	 * @return the parsed {@link ExpressionQuery}.
	 */
	public static ExpressionQuery create(String query) {

		List<ParameterBinding> parameterBindings = new ArrayList<>();
		String rewritten = transformQueryAndCollectExpressionParametersIntoBindings(query, parameterBindings);

		return new ExpressionQuery(rewritten, parameterBindings);
	}

	public String getQuery() {
		return query;
	}

	public List<ParameterBinding> getBindings() {
		return parameterBindings;
	}

	private static String transformQueryAndCollectExpressionParametersIntoBindings(String input,
			List<ParameterBinding> bindings) {

		StringBuilder result = new StringBuilder();

		int startIndex = 0;
		int currentPosition = 0;
		int parameterIndex = 0;

		while (currentPosition < input.length()) {

			Matcher matcher = findNextBindingOrExpression(input, currentPosition);

			// no expression parameter found
			if (matcher == null) {
				break;
			}

			int exprStart = matcher.start();
			currentPosition = exprStart;

			// eat parameter expression
			int curlyBraceOpenCount = 1;
			currentPosition += 3;

			while (curlyBraceOpenCount > 0 && currentPosition < input.length()) {
				switch (input.charAt(currentPosition++)) {
					case CURLY_BRACE_OPEN:
						curlyBraceOpenCount++;
						break;
					case CURLY_BRACE_CLOSE:
						curlyBraceOpenCount--;
						break;
					default:
				}
			}

			result.append(input.subSequence(startIndex, exprStart));

			String parameterName = String.format(SYNTHETIC_PARAMETER_TEMPLATE, parameterIndex++);
			result.append(':').append(parameterName);

			bindings.add(ParameterBinding.named(parameterName, matcher.group(1)));

			currentPosition = matcher.end();
			startIndex = currentPosition;
		}

		return result.append(input.subSequence(currentPosition, input.length())).toString();
	}

	@Nullable
	private static Matcher findNextBindingOrExpression(String input, int position) {

		Matcher matcher = EXPRESSION_BINDING_PATTERN.matcher(input);
		if (matcher.find(position)) {
			return matcher;
		}

		return null;
	}

	@Override
	public String toString() {
		return query;
	}

	/**
	 * A SpEL parameter binding.
	 *
	 * @author Mark Paluch
	 */
	static class ParameterBinding {

		private final String parameterName;
		private final String expression;

		private ParameterBinding(String parameterName, String expression) {

			this.expression = expression;
			this.parameterName = parameterName;
		}

		static ParameterBinding named(String name, String expression) {
			return new ParameterBinding(name, expression);
		}

		String getExpression() {
			return expression;
		}

		String getParameterName() {
			return parameterName;
		}
	}
}
