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

import org.springframework.data.repository.query.SpelQueryContext;

/**
 * Query using Spring Expression Language to indicate parameter bindings. Queries using SpEL use {@code :#{…}} to
 * enclose expressions. Expressions are substituted with synthetic named parameters and require therefore enabled named
 * parameter expansion for execution.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class ExpressionQuery {

	private static final String SYNTHETIC_PARAMETER_TEMPLATE = "__synthetic_%d__";

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

		SpelQueryContext queryContext = SpelQueryContext.of((counter, expression) -> {

			String parameterName = String.format(SYNTHETIC_PARAMETER_TEMPLATE, counter);
			parameterBindings.add(new ParameterBinding(parameterName, expression));
			return parameterName;
		}, String::concat);

		SpelQueryContext.SpelExtractor parsed = queryContext.parse(query);

		return new ExpressionQuery(parsed.getQueryString(), parameterBindings);
	}

	public String getQuery() {
		return query;
	}

	public List<ParameterBinding> getBindings() {
		return parameterBindings;
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

		String getExpression() {
			return expression;
		}

		String getParameterName() {
			return parameterName;
		}
	}
}
