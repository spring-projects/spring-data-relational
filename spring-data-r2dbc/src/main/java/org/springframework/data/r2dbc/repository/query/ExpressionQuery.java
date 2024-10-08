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

import java.util.Map;

import org.springframework.data.expression.ValueExpression;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.repository.query.ValueExpressionQueryRewriter;

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
	private final Map<String, ValueExpression> parameterMap;

	private ExpressionQuery(String query, Map<String, ValueExpression> parameterMap) {
		this.query = query;
		this.parameterMap = parameterMap;
	}

	/**
	 * Create a {@link ExpressionQuery} from a {@code query}.
	 *
	 * @param query the query string to parse.
	 * @return the parsed {@link ExpressionQuery}.
	 */
	public static ExpressionQuery create(ValueExpressionParser parser, String query) {


		ValueExpressionQueryRewriter rewriter = ValueExpressionQueryRewriter.of(parser,
				(counter, expression) -> String.format(SYNTHETIC_PARAMETER_TEMPLATE, counter), String::concat);
		ValueExpressionQueryRewriter.ParsedQuery parsed = rewriter.parse(query);

		return new ExpressionQuery(parsed.getQueryString(), parsed.getParameterMap());
	}

	public String getQuery() {
		return query;
	}

	public Map<String, ValueExpression> getBindings() {
		return parameterMap;
	}

	@Override
	public String toString() {
		return query;
	}

}
