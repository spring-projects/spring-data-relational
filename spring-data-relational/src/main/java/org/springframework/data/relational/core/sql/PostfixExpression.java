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
package org.springframework.data.relational.core.sql;

/**
 * @author Mark Paluch
 */
public class PostfixExpression extends AbstractSegment implements Expression {

	private final Expression expression;
	private final Expression postfix;

	public PostfixExpression(Expression expression, Expression postfix) {
		super(expression, postfix);
		this.expression = expression;
		this.postfix = postfix;
	}

	public Expression getExpression() {
		return expression;
	}

	public Expression getPostfix() {
		return postfix;
	}

	@Override
	public String toString() {
		return expression.toString() + postfix;
	}
}
