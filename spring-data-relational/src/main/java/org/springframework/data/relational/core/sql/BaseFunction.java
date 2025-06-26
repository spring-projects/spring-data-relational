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

import java.util.Collections;
import java.util.List;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Simple function accepting one or more {@link Expression}s and definition how parentheses should be rendered.
 *
 * @author Mark Paluch
 * @since 4.0
 */
public class BaseFunction extends AbstractSegment implements Expression {

	private final String functionName;
	private final String beforeArgs;
	private final String afterArgs;

	private final List<? extends Expression> expressions;

	protected BaseFunction(String functionName, String beforeArgs, String afterArgs,
			List<? extends Expression> expressions) {

		super(expressions.toArray(new Expression[0]));

		this.functionName = functionName;
		this.beforeArgs = beforeArgs;
		this.afterArgs = afterArgs;
		this.expressions = expressions;
	}

	/**
	 * Creates a new {@link BaseFunction} given {@code functionName} and {@link List} of {@link Expression}s.
	 *
	 * @param functionName must not be {@literal null}.
	 * @param expressions zero or many {@link Expression}s, must not be {@literal null}.
	 * @return
	 */
	public static BaseFunction create(String functionName, String openParenthesis, String closeParenthesis,
			List<? extends Expression> expressions) {

		Assert.hasText(functionName, "Function name must not be null or empty");
		Assert.notNull(expressions, "Expressions name must not be null");

		return new BaseFunction(functionName, openParenthesis, closeParenthesis, expressions);
	}

	/**
	 * Expose this function result under a column {@code alias}.
	 *
	 * @param alias column alias name, must not {@literal null} or empty.
	 * @return the aliased {@link BaseFunction}.
	 */
	public BaseFunction as(String alias) {

		Assert.hasText(alias, "Alias must not be null or empty");

		return new AliasedFunction(this, SqlIdentifier.unquoted(alias));
	}

	/**
	 * Expose this function result under a column {@code alias}.
	 *
	 * @param alias column alias name, must not {@literal null}.
	 * @return the aliased {@link BaseFunction}.
	 * @since 2.0
	 */
	public BaseFunction as(SqlIdentifier alias) {

		Assert.notNull(alias, "Alias must not be null");

		return new AliasedFunction(this, alias);
	}

	/**
	 * @return the function name.
	 */
	public String getFunctionName() {
		return functionName;
	}

	public String getBeforeArgs() {
		return beforeArgs;
	}

	public String getAfterArgs() {
		return afterArgs;
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
		return functionName + getBeforeArgs() + StringUtils.collectionToDelimitedString(expressions, ", ") + getAfterArgs();
	}

	/**
	 * {@link Aliased} {@link BaseFunction} implementation.
	 */
	static class AliasedFunction extends BaseFunction implements Aliased {

		private final SqlIdentifier alias;

		AliasedFunction(BaseFunction function, SqlIdentifier alias) {
			super(function.getFunctionName(), function.getBeforeArgs(), function.getAfterArgs(), function.expressions);
			this.alias = alias;
		}

		@Override
		public SqlIdentifier getAlias() {
			return alias;
		}
	}
}
