/*
 * Copyright 2019-2025 the original author or authors.
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

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

/**
 * Wrapper for multiple {@link Expression}s.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @since 4.0
 */
public abstract class MultipleExpression extends AbstractSegment {

	private final List<Expression> expressions;
	private final String delimiter;

	MultipleExpression(String delimiter, Expression... expressions) {

		super(expressions);

		this.delimiter = delimiter;
		this.expressions = Arrays.asList(expressions);
	}

	public List<Expression> getExpressions() {
		return expressions;
	}

	@Override
	public String toString() {

		StringJoiner joiner = new StringJoiner(delimiter);
		expressions.forEach(c -> joiner.add(c.toString()));
		return joiner.toString();
	}
}
