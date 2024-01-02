/*
 * Copyright 2019-2024 the original author or authors.
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

import java.util.Collections;
import java.util.List;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Simple function accepting one or more {@link Expression}s.
 *
 * @author Mark Paluch
 * @since 1.1
 */
public class SimpleFunction extends AbstractSegment implements Expression {

	private final String functionName;
	private final List<? extends Expression> expressions;

	private SimpleFunction(String functionName, List<? extends Expression> expressions) {

		super(expressions.toArray(new Expression[0]));

		this.functionName = functionName;
		this.expressions = expressions;
	}

	/**
	 * Creates a new {@link SimpleFunction} given {@code functionName} and {@link List} of {@link Expression}s.
	 *
	 * @param functionName must not be {@literal null}.
	 * @param expressions zero or many {@link Expression}s, must not be {@literal null}.
	 * @return
	 */
	public static SimpleFunction create(String functionName, List<? extends Expression> expressions) {

		Assert.hasText(functionName, "Function name must not be null or empty");
		Assert.notNull(expressions, "Expressions name must not be null");

		return new SimpleFunction(functionName, expressions);
	}

	/**
	 * Expose this function result under a column {@code alias}.
	 *
	 * @param alias column alias name, must not {@literal null} or empty.
	 * @return the aliased {@link SimpleFunction}.
	 */
	public SimpleFunction as(String alias) {

		Assert.hasText(alias, "Alias must not be null or empty");

		return new AliasedFunction(functionName, expressions, SqlIdentifier.unquoted(alias));
	}

	/**
	 * Expose this function result under a column {@code alias}.
	 *
	 * @param alias column alias name, must not {@literal null}.
	 * @return the aliased {@link SimpleFunction}.
	 * @since 2.0
	 */
	public SimpleFunction as(SqlIdentifier alias) {

		Assert.notNull(alias, "Alias must not be null");

		return new AliasedFunction(functionName, expressions, alias);
	}

	/**
	 * @return the function name.
	 */
	public String getFunctionName() {
		return functionName;
	}

	/**
	 * @return the function arguments.
	 * @since 2.0
	 */
	public List<Expression> getExpressions() {
		return Collections.unmodifiableList(expressions);
	}

	@Override
	public String toString() {
		return functionName + "(" + StringUtils.collectionToDelimitedString(expressions, ", ") + ")";
	}

	/**
	 * {@link Aliased} {@link SimpleFunction} implementation.
	 */
	static class AliasedFunction extends SimpleFunction implements Aliased {

		private final SqlIdentifier alias;

		AliasedFunction(String functionName, List<? extends Expression> expressions, SqlIdentifier alias) {
			super(functionName, expressions);
			this.alias = alias;
		}

		@Override
		public SqlIdentifier getAlias() {
			return alias;
		}
	}
}
